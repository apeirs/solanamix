// Command solanamix: Solana JSON-RPC and PubSub over the Nym mixnet.
//
//	solanamix start             start nym-socks5-client and the local gateway in the background
//	solanamix serve             local gateway: http://127.0.0.1:8899 (+ ws://127.0.0.1:8900), /devnet, /testnet
//	solanamix slot|info|balance|send-raw|rpc|watch   talk to a public RPC directly over the mixnet
//	solanamix probe             which public upstreams answer Nym exit traffic, and how fast
//	solanamix ip                which IP do upstreams see? (a Nym exit gateway)
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/apeirs/solanamix/gateway"
	"github.com/apeirs/solanamix/mixnet"
	"github.com/apeirs/solanamix/sol"
)

const version = "0.1.0"

var start = time.Now()

func tracef(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[%7.2fs] "+format+"\n", append([]any{time.Since(start).Seconds()}, args...)...)
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "solanamix:", err)
	os.Exit(1)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// multi is a repeatable string flag.
type multi []string

func (m *multi) String() string { return strings.Join(*m, ",") }
func (m *multi) Set(s string) error {
	if s = strings.TrimSpace(s); s != "" {
		*m = append(*m, s)
	}
	return nil
}

type common struct {
	proxy, dns, network, apiURL string
	add                         multi
	verbose, jsonOut            bool
	timeout                     time.Duration
	reg                         *sol.Registry
	nw                          *sol.Network
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.proxy, "proxy", envOr("SOLANAMIX_PROXY", "127.0.0.1:1095"), "nym-socks5-client SOCKS5 address ($SOLANAMIX_PROXY)")
	fs.StringVar(&c.dns, "dns", envOr("SOLANAMIX_DNS", mixnet.DNSDoH), "name resolution: doh (DoH over the mixnet, connect by IPv4) or exit")
	fs.StringVar(&c.network, "network", envOr("SOLANAMIX_NETWORK", "mainnet"), "mainnet (mainnet-beta), devnet, testnet, or one added with --add")
	fs.StringVar(&c.apiURL, "api", "", "use this https RPC URL instead of the network's list (still genesis checked)")
	fs.Var(&c.add, "add", "network upstreams: name=https://rpc[+wss://ws][|https://rpc2] (repeatable; mainnet=... replaces the built-in list)")
	fs.BoolVar(&c.verbose, "v", false, "trace every mixnet step to stderr")
	fs.BoolVar(&c.jsonOut, "json", false, "JSON output")
	fs.DurationVar(&c.timeout, "timeout", 5*time.Minute, "overall timeout for the command")
}

func (c *common) registry() *sol.Registry {
	if c.reg != nil {
		return c.reg
	}
	c.reg = sol.NewRegistry()
	for _, a := range c.add {
		if err := c.reg.ParseAdd(a); err != nil {
			die(fmt.Errorf("--add: %w", err))
		}
	}
	return c.reg
}

func (c *common) dialer() *mixnet.Dialer {
	proxy, err := mixnet.ParseProxy(c.proxy)
	if err != nil {
		die(err)
	}
	if c.dns != mixnet.DNSDoH && c.dns != mixnet.DNSExit {
		die(fmt.Errorf("--dns must be doh or exit"))
	}
	d := &mixnet.Dialer{Proxy: proxy, DNS: c.dns}
	if c.verbose {
		d.Trace = tracef
	}
	return d
}

func (c *common) client() *sol.Client {
	cl := &sol.Client{D: c.dialer()}
	if c.verbose {
		cl.Log = tracef
	}
	return cl
}

// net is the selected network; with --api, the same network (and genesis)
// with that one upstream.
func (c *common) net() *sol.Network {
	if c.nw != nil {
		return c.nw
	}
	n, ok := c.registry().Get(c.network)
	if c.apiURL != "" {
		u, err := sol.ParseUpstream(c.apiURL)
		if err != nil {
			die(err)
		}
		name := strings.ToLower(c.network)
		cp := &sol.Network{Name: name, Upstreams: []sol.Upstream{u}}
		if ok {
			cp.Name, cp.Genesis, cp.Tokens = n.Name, n.GenesisPinned(), n.Tokens
		}
		c.nw = cp
		return cp
	}
	if !ok {
		die(fmt.Errorf("unknown network %q (see `solanamix networks`), or pass --api URL", c.network))
	}
	if len(n.Upstreams) == 0 {
		die(fmt.Errorf("network %s has no upstream; pass --api URL", n.Name))
	}
	c.nw = n
	return n
}

// only is n restricted to the upstream that already answered, so follow-up
// calls of one command see one consistent node. It shares n's name, so the
// genesis check is not repeated.
func only(n *sol.Network, used string) *sol.Network {
	cp := &sol.Network{Name: n.Name, Genesis: n.GenesisPinned(), Tokens: n.Tokens}
	for _, u := range n.Upstreams {
		if u.RPC == used {
			cp.Upstreams = []sol.Upstream{u}
		}
	}
	if cp.Upstreams == nil {
		cp.Upstreams = n.Upstreams
	}
	return cp
}

func (c *common) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	return ctx, func() { stop(); cancel() }
}

func (c *common) out(v any, human string) {
	if c.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(v)
		return
	}
	fmt.Println(human)
}

// parse allows flags after positional arguments. A lone "-" is positional.
func parse(fs *flag.FlagSet, args []string) []string {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			os.Exit(2)
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `solanamix `+version+`: Solana JSON-RPC and PubSub through the Nym mixnet

Quick start (mixnet client + local gateway in the background):
  solanamix start [--network devnet]        then: solana config set --url http://127.0.0.1:8899
  solanamix stop | status

Mixnet client (official nym-socks5-client):
  solanamix nym setup [--binary PATH] [--port 1095]   init on a random active exit gateway, then start
  solanamix nym start | stop | status | exits

Local gateway for wallets and tools (Solana CLI, @solana/web3.js, curl ...):
  solanamix serve [--listen 127.0.0.1:8899] [--listen-ws 127.0.0.1:8900] [--default mainnet]
               [--add name=https://rpc|https://rpc2] [--allow-origin https://app.example]
     mainnet  http://127.0.0.1:8899           ws://127.0.0.1:8900
     devnet   http://127.0.0.1:8899/devnet    ws://127.0.0.1:8900/devnet   (also /testnet, /mainnet)

Talk to a public Solana RPC directly over the mixnet:
  solanamix slot                              current slot and block height
  solanamix info                              genesis check, version, epoch, slot
  solanamix balance PUBKEY [--all-tokens]     SOL, USDC/USDT and other SPL token balances
  solanamix send-raw TX|FILE|- [--skip-preflight]   forward a signed transaction (base64 or base58);
                                           checks its signatures locally first; holds no keys
  solanamix rpc METHOD ['[params JSON]']      any JSON-RPC method
  solanamix watch slot|account PUBKEY|signature SIG [--count N]   PubSub over the mixnet
  solanamix networks                          built-in networks and upstreams
  solanamix probe [--network all]             which upstreams work through the mixnet, and how fast
  solanamix ip                                which IP do upstreams see? (should be a Nym exit gateway)

Common flags: --network mainnet|devnet|testnet --api URL --proxy 127.0.0.1:1095 --dns doh|exit -v --json --timeout 5m
solanamix never holds a private key, and there is no clearnet fallback: if the
mixnet client is down, solanamix refuses to connect.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "start":
		cmdStart(args)
	case "stop":
		cmdStop(args)
	case "status":
		cmdStatus(args)
	case "nym":
		cmdNym(args)
	case "serve":
		cmdServe(args)
	case "slot":
		cmdSlot(args)
	case "info":
		cmdInfo(args)
	case "balance":
		cmdBalance(args)
	case "send-raw":
		cmdSendRaw(args)
	case "rpc":
		cmdRPC(args)
	case "watch":
		cmdWatch(args)
	case "networks":
		cmdNetworks(args)
	case "probe":
		cmdProbe(args)
	case "ip":
		cmdIP(args)
	case "version", "-version", "--version":
		fmt.Println("solanamix", version)
	default:
		usage()
		os.Exit(2)
	}
}

func cmdNetworks(args []string) {
	var c common
	fs := flag.NewFlagSet("networks", flag.ExitOnError)
	c.register(fs)
	parse(fs, args)
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "NETWORK\tGENESIS HASH\tGATEWAY PATH\tUPSTREAMS (in order; PubSub at the wss:// of each unless shown)")
	for _, n := range c.registry().All() {
		g := n.GenesisPinned()
		if g == "" {
			g = "(pinned on first use)"
		}
		var ups []string
		for _, u := range n.Upstreams {
			s := strings.TrimPrefix(u.RPC, "https://")
			if u.WS != "" {
				s += " (" + u.WS + ")"
			}
			ups = append(ups, s)
		}
		fmt.Fprintf(tw, "%s\t%s\t/%s\t%s\n", n.Name, g, n.Name, strings.Join(ups, ", "))
	}
	tw.Flush()
}

func loopback(addr string) bool {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(h)
	return h == "localhost" || (ip != nil && ip.IsLoopback())
}

func cmdServe(args []string) {
	var c common
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	c.register(fs)
	listen := fs.String("listen", "127.0.0.1:8899", "JSON-RPC (and websocket) address: the default network at /, every network at /<name>")
	listenWS := fs.String("listen-ws", "127.0.0.1:8900", "PubSub websocket address (clients use the RPC port + 1); \"\" = none")
	def := fs.String("default", "mainnet", "network served at the root (\"\" = none)")
	hosts := fs.String("allow-host", "", "extra Host header values to accept (comma separated)")
	origins := fs.String("allow-origin", "", "web origins allowed to use the gateway, comma separated (\"*\" = any page; browser extensions and non-browser clients are always allowed)")
	parse(fs, args)
	reg := c.registry()
	defName := *def
	if defName != "" {
		n, ok := reg.Get(defName)
		if !ok {
			die(fmt.Errorf("unknown --default network %q", *def))
		}
		defName = n.Name
	}
	cl := c.client()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g := &gateway.Gateway{Client: cl, Nets: reg, Default: defName, Log: logf}
	if *hosts != "" {
		g.Hosts = strings.Split(*hosts, ",")
	}
	if *origins != "" {
		g.Origins = strings.Split(*origins, ",")
	}
	addrs := []string{*listen}
	if *listenWS != "" {
		addrs = append(addrs, *listenWS)
	}
	var wg sync.WaitGroup
	for i, a := range addrs {
		ln, err := net.Listen("tcp", a)
		if err != nil {
			die(err)
		}
		if !loopback(a) {
			fmt.Fprintf(os.Stderr, "WARNING: %s is not bound to loopback; anyone who can reach it can use your mixnet client\n", a)
		}
		if i == 0 {
			logf("solanamix gateway on http://%s  (root = %s; SOCKS5 %s)", ln.Addr(), defName, c.proxy)
		} else {
			logf("solanamix PubSub on ws://%s", ln.Addr())
		}
		srv := &http.Server{Handler: g, ReadHeaderTimeout: 30 * time.Second}
		go func() { <-ctx.Done(); srv.Close() }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				die(err)
			}
		}()
	}
	wg.Wait()
}

type slotInfo struct {
	Slot        uint64 `json:"slot"`
	BlockHeight uint64 `json:"block_height"`
	Upstream    string `json:"upstream"`
}

func getSlot(ctx context.Context, cl *sol.Client, n *sol.Network) (slotInfo, error) {
	var s slotInfo
	cfg := []any{map[string]string{"commitment": "confirmed"}}
	used, err := cl.Call(ctx, n, "getSlot", cfg, &s.Slot)
	if err != nil {
		return s, err
	}
	s.Upstream = used
	// Finalized height: publicnode answered getBlockHeight at "confirmed"
	// with the slot number (2026-11-08).
	_, err = cl.Call(ctx, only(n, used), "getBlockHeight", nil, &s.BlockHeight)
	return s, err
}

func cmdSlot(args []string) {
	var c common
	fs := flag.NewFlagSet("slot", flag.ExitOnError)
	c.register(fs)
	parse(fs, args)
	ctx, cancel := c.ctx()
	defer cancel()
	cl := c.client()
	n := c.net()
	s, err := getSlot(ctx, cl, n)
	if err != nil {
		die(err)
	}
	c.out(map[string]any{"network": n.Name, "slot": s.Slot, "block_height": s.BlockHeight, "genesis_verified": cl.Verified(n, s.Upstream), "upstream": s.Upstream},
		fmt.Sprintf("%s: slot %d (confirmed), block height %d (finalized); genesis verified, via %s over the mixnet", n.Name, s.Slot, s.BlockHeight, sol.HostOf(s.Upstream)))
}

func cmdInfo(args []string) {
	var c common
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	c.register(fs)
	parse(fs, args)
	ctx, cancel := c.ctx()
	defer cancel()
	cl := c.client()
	n := c.net()
	var ver struct {
		Core string `json:"solana-core"`
	}
	used, err := cl.Call(ctx, n, "getVersion", nil, &ver)
	if err != nil {
		die(err)
	}
	one := only(n, used)
	var ep struct {
		Epoch        uint64 `json:"epoch"`
		SlotIndex    uint64 `json:"slotIndex"`
		SlotsInEpoch uint64 `json:"slotsInEpoch"`
		AbsoluteSlot uint64 `json:"absoluteSlot"`
		BlockHeight  uint64 `json:"blockHeight"`
		TxCount      uint64 `json:"transactionCount"`
	}
	if _, err := cl.Call(ctx, one, "getEpochInfo", nil, &ep); err != nil {
		die(err)
	}
	var health string
	herr := func() error { _, err := cl.Call(ctx, one, "getHealth", nil, &health); return err }()
	if herr != nil {
		health = "not ok: " + herr.Error()
	}
	res := map[string]any{"network": n.Name, "genesis": n.GenesisPinned(), "genesis_verified": cl.Verified(n, used), "solana_core": ver.Core,
		"epoch": ep.Epoch, "slot": ep.AbsoluteSlot, "block_height": ep.BlockHeight, "transaction_count": ep.TxCount, "health": health, "upstream": used}
	human := fmt.Sprintf("%s: slot %d, block height %d, epoch %d (%.1f%% through), solana-core %s, health %s\n  genesis %s verified (via %s over the mixnet)",
		n.Name, ep.AbsoluteSlot, ep.BlockHeight, ep.Epoch, 100*float64(ep.SlotIndex)/float64(max(ep.SlotsInEpoch, 1)), ver.Core, health, n.GenesisPinned(), sol.HostOf(used))
	if ep.TxCount > 0 {
		human += fmt.Sprintf("\n  %d transactions since genesis", ep.TxCount)
	}
	c.out(res, human)
}

var tokenPrograms = []struct{ name, id string }{
	{"SPL Token", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
	{"Token-2022", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"},
}

func cmdBalance(args []string) {
	var c common
	fs := flag.NewFlagSet("balance", flag.ExitOnError)
	c.register(fs)
	noTokens := fs.Bool("no-tokens", false, "skip the SPL token balances")
	allTokens := fs.Bool("all-tokens", false, "list every token account, not only the known stablecoins")
	pos := parse(fs, args)
	if len(pos) != 1 {
		die(fmt.Errorf("usage: solanamix balance PUBKEY [--network devnet] [--no-tokens]"))
	}
	if _, err := sol.ParsePubkey(pos[0]); err != nil {
		die(err)
	}
	addr := strings.TrimSpace(pos[0])
	ctx, cancel := c.ctx()
	defer cancel()
	cl := c.client()
	n := c.net()
	var bal struct {
		Context struct {
			Slot uint64 `json:"slot"`
		} `json:"context"`
		Value uint64 `json:"value"`
	}
	used, err := cl.Call(ctx, n, "getBalance", []any{addr, map[string]string{"commitment": "confirmed"}}, &bal)
	if err != nil {
		die(err)
	}
	res := map[string]any{"address": addr, "network": n.Name, "lamports": bal.Value, "slot": bal.Context.Slot, "upstream": used}
	lines := []string{"  " + sol.Lamports(bal.Value) + fmt.Sprintf(" (%d lamports, slot %d)", bal.Value, bal.Context.Slot)}
	if !*noTokens {
		known := map[string]string{}
		for _, t := range n.Tokens {
			known[t.Mint] = t.Symbol
		}
		tokens := map[string]string{}
		for _, p := range tokenPrograms {
			var r struct {
				Value []struct {
					Account struct {
						Data struct {
							Parsed struct {
								Info struct {
									Mint        string `json:"mint"`
									TokenAmount struct {
										UI       string `json:"uiAmountString"`
										Decimals int    `json:"decimals"`
									} `json:"tokenAmount"`
								} `json:"info"`
							} `json:"parsed"`
						} `json:"data"`
					} `json:"account"`
				} `json:"value"`
			}
			// The full upstream list, not just the one that answered
			// getBalance: some public endpoints refuse indexed calls (403).
			tused, err := cl.Call(ctx, n, "getTokenAccountsByOwner", []any{addr, map[string]string{"programId": p.id},
				map[string]string{"encoding": "jsonParsed", "commitment": "confirmed"}}, &r)
			if err != nil {
				lines = append(lines, fmt.Sprintf("  %s accounts: %v", p.name, err))
				continue
			}
			// One line per mint: a wallet can hold several accounts of one token.
			type sum struct {
				total    *big.Rat
				decimals int
				accounts int
			}
			sums := map[string]*sum{}
			var order []string
			for _, a := range r.Value {
				in := a.Account.Data.Parsed.Info
				v, ok := new(big.Rat).SetString(in.TokenAmount.UI)
				if !ok {
					continue
				}
				if sums[in.Mint] == nil {
					sums[in.Mint] = &sum{new(big.Rat), in.TokenAmount.Decimals, 0}
					order = append(order, in.Mint)
				}
				sums[in.Mint].total.Add(sums[in.Mint].total, v)
				sums[in.Mint].accounts++
			}
			other := 0
			for _, mint := range order {
				sm := sums[mint]
				amt := strings.TrimSuffix(strings.TrimRight(sm.total.FloatString(sm.decimals), "0"), ".")
				if amt == "" {
					amt = "0"
				}
				tokens[mint] = amt
				sym := known[mint]
				if sym == "" && !*allTokens {
					if sm.total.Sign() != 0 {
						other++
					}
					continue
				}
				if sym == "" {
					sym = "mint " + mint
				}
				accts := ""
				if sm.accounts > 1 {
					accts = fmt.Sprintf(", %d accounts", sm.accounts)
				}
				lines = append(lines, fmt.Sprintf("  %s %s (%s%s)", amt, sym, p.name, accts))
			}
			if other > 0 {
				lines = append(lines, fmt.Sprintf("  + %d other tokens with a non-zero %s balance (--all-tokens lists them)", other, p.name))
			}
			if len(r.Value) == 0 {
				lines = append(lines, fmt.Sprintf("  no %s accounts", p.name))
			}
			if tused != used {
				lines = append(lines, fmt.Sprintf("  (%s accounts via %s)", p.name, sol.HostOf(tused)))
			}
		}
		res["tokens"] = tokens
	}
	c.out(res, fmt.Sprintf("%s on %s (via %s over the mixnet):\n%s", addr, n.Name, sol.HostOf(used), strings.Join(lines, "\n")))
}

func readArg(raw string) string {
	if raw == "-" {
		b, _ := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
		return string(b)
	}
	if st, err := os.Stat(raw); err == nil && !st.IsDir() && st.Size() < 1<<20 {
		b, err := os.ReadFile(raw)
		if err != nil {
			die(err)
		}
		return string(b)
	}
	return raw
}

func cmdSendRaw(args []string) {
	var c common
	fs := flag.NewFlagSet("send-raw", flag.ExitOnError)
	c.register(fs)
	skip := fs.Bool("skip-preflight", false, "ask the node not to simulate the transaction first")
	noCheck := fs.Bool("no-verify", false, "don't check the signatures locally (a node rejects bad ones anyway)")
	pos := parse(fs, args)
	if len(pos) != 1 {
		die(fmt.Errorf("usage: solanamix send-raw TX_BASE64 | TX_BASE58 | FILE | -   (a signed, serialized transaction)"))
	}
	tx, err := sol.ParseTxInput(readArg(pos[0]))
	if err != nil {
		die(err)
	}
	if !*noCheck {
		if err := tx.CheckBroadcastable(); err != nil {
			die(err)
		}
	}
	ctx, cancel := c.ctx()
	defer cancel()
	n := c.net()
	kind := "legacy"
	if tx.Version == 0 {
		kind = "v0"
	}
	desc := fmt.Sprintf("%s tx, %d instruction(s), %d signature(s), fee payer %s", kind, tx.Instructions, len(tx.Signatures), tx.Signer())
	var sig string
	used, err := c.client().Call(ctx, n, "sendTransaction", []any{base64.StdEncoding.EncodeToString(tx.Bytes()),
		map[string]any{"encoding": "base64", "skipPreflight": *skip, "preflightCommitment": "confirmed"}}, &sig)
	if err != nil {
		var re *sol.RPCError
		msg := err.Error()
		var logs []string
		if errors.As(err, &re) {
			logs = re.Logs()
		}
		res := map[string]any{"error": msg, "signature": tx.ID(), "upstream": used, "logs": logs}
		human := fmt.Sprintf("rejected: %s\n  signature %s (%s)", msg, tx.ID(), desc)
		for _, l := range logs {
			human += "\n  log: " + l
		}
		c.out(res, human)
		os.Exit(1)
	}
	c.out(map[string]any{"signature": sig, "upstream": used},
		fmt.Sprintf("sent: %s\n  (%s) via %s over the mixnet; check it with: solanamix rpc getSignatureStatuses '[[\"%s\"]]'", sig, desc, sol.HostOf(used), sig))
}

func cmdRPC(args []string) {
	var c common
	fs := flag.NewFlagSet("rpc", flag.ExitOnError)
	c.register(fs)
	pos := parse(fs, args)
	if len(pos) < 1 || len(pos) > 2 {
		die(fmt.Errorf("usage: solanamix rpc METHOD ['[params as a JSON array]']   e.g. solanamix rpc getLatestBlockhash"))
	}
	var params any
	if len(pos) == 2 {
		raw := readArg(pos[1])
		if err := json.Unmarshal([]byte(raw), &params); err != nil {
			die(fmt.Errorf("params must be JSON: %w", err))
		}
	}
	ctx, cancel := c.ctx()
	defer cancel()
	var out json.RawMessage
	used, err := c.client().Call(ctx, c.net(), pos[0], params, &out)
	if err != nil {
		die(err)
	}
	var pretty any
	_ = json.Unmarshal(out, &pretty)
	b, _ := json.MarshalIndent(pretty, "", "  ")
	fmt.Println(string(b))
	fmt.Fprintf(os.Stderr, "(via %s over the mixnet)\n", sol.HostOf(used))
}

type probeRow struct {
	network, upstream, rpc, ws string
	rpcS, wsS                  float64
	ok                         bool
}

func cmdProbe(args []string) {
	var c common
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	c.register(fs)
	par := fs.Int("parallel", 3, "upstreams probed at once")
	noWS := fs.Bool("no-ws", false, "skip the PubSub websocket check")
	parse(fs, args)
	if !flagSet(fs, "network") && os.Getenv("SOLANAMIX_NETWORK") == "" && c.apiURL == "" {
		c.network = "all"
	}
	ctx, cancel := c.ctx()
	defer cancel()
	cl := c.client()
	var nets []*sol.Network
	if c.network == "all" {
		nets = c.registry().All()
	} else {
		nets = []*sol.Network{c.net()}
	}
	type job struct {
		n  *sol.Network
		up sol.Upstream
		i  int
	}
	var jobs []job
	for _, n := range nets {
		for _, u := range n.Upstreams {
			jobs = append(jobs, job{n, u, len(jobs)})
		}
	}
	rows := make([]probeRow, len(jobs))
	sem := make(chan struct{}, max(1, *par))
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			one := &sol.Network{Name: j.n.Name, Genesis: j.n.GenesisPinned(), Upstreams: []sol.Upstream{j.up}}
			r := probeRow{network: j.n.Name, upstream: strings.TrimPrefix(j.up.RPC, "https://")}
			t0 := time.Now()
			var slot uint64
			if _, err := cl.Call(ctx, one, "getSlot", nil, &slot); err != nil {
				r.rpc = short(err)
			} else {
				r.rpc = fmt.Sprintf("genesis ok, slot %d", slot)
				r.ok = true
			}
			r.rpcS = time.Since(t0).Seconds()
			if *noWS || !r.ok {
				r.ws = "-"
			} else {
				t1 := time.Now()
				r.ws = probeWS(ctx, cl, one)
				r.wsS = time.Since(t1).Seconds()
			}
			rows[j.i] = r
		}(j)
	}
	wg.Wait()
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "NETWORK\tUPSTREAM\tGENESIS CHECK + getSlot\tTIME\tPUBSUB slotSubscribe\tTIME")
	ok := 0
	for _, r := range rows {
		if r.ok {
			ok++
		}
		ws := "-"
		if r.ws != "-" {
			ws = fmt.Sprintf("%.1fs", r.wsS)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%.1fs\t%s\t%s\n", r.network, r.upstream, r.rpc, r.rpcS, r.ws, ws)
	}
	tw.Flush()
	fmt.Fprintf(os.Stderr, "%d of %d upstreams passed the genesis check and answered through the mixnet\n", ok, len(rows))
}

// probeWS opens the upstream's PubSub socket and waits for one slot
// notification.
func probeWS(ctx context.Context, cl *sol.Client, n *sol.Network) string {
	got := ""
	err := subscribe(ctx, cl, n, "slotSubscribe", nil, func(v json.RawMessage) bool {
		var s struct {
			Slot uint64 `json:"slot"`
		}
		_ = json.Unmarshal(v, &s)
		got = fmt.Sprintf("slot %d notified", s.Slot)
		return false
	})
	if err != nil {
		return short(err)
	}
	return got
}

func flagSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

func short(err error) string {
	e := strings.ReplaceAll(err.Error(), "\n", "; ")
	if len(e) > 70 {
		e = e[:70]
	}
	return "FAIL " + e
}

func cmdIP(args []string) {
	var c common
	fs := flag.NewFlagSet("ip", flag.ExitOnError)
	c.register(fs)
	noLookup := fs.Bool("no-lookup", false, "don't look the IP up in the nym-api node list")
	parse(fs, args)
	d := c.dialer()
	ctx, cancel := c.ctx()
	defer cancel()
	showIP(ctx, &c, d, *noLookup)
}
