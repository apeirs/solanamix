// Package sol talks to public Solana JSON-RPC and PubSub (websocket) endpoints
// through the Nym mixnet.
package sol

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
)

// Upstream is one public RPC endpoint. WS is its PubSub websocket URL; empty
// means the RPC URL with wss:// (how the public endpoints serve it).
type Upstream struct {
	RPC string
	WS  string
}

// WSURL is the PubSub URL of the upstream.
func (u Upstream) WSURL() string {
	if u.WS != "" {
		return u.WS
	}
	return "wss://" + strings.TrimPrefix(u.RPC, "https://")
}

// Token is an SPL token that `balance` names by symbol.
type Token struct {
	Symbol string
	Mint   string
}

// Network is a Solana cluster with public, keyless upstreams (tried in order).
type Network struct {
	Name string
	// Genesis is the cluster's genesis hash (base58). Every upstream is
	// checked against it with getGenesisHash before it gets a request; a
	// custom network pins it on first use.
	Genesis   string
	Upstreams []Upstream
	Tokens    []Token
}

// Networks are the built-in clusters (checked 2026-11-08 through the mixnet).
// Any other one (a private cluster, your own node) can be added with
// --add name=https://host.
var Networks = []Network{
	{"mainnet", "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
		[]Upstream{
			{RPC: "https://solana-rpc.publicnode.com"},        // Allnodes
			{RPC: "https://public.rpc.solanavibestation.com"}, // Solana Vibe Station
			// Solana Foundation. Last: on 2026-11-08 it answered solanamix through
			// a Nym exit with 403 "Your IP or provider is blocked from this
			// endpoint" (curl through the same exit got 200).
			{RPC: "https://api.mainnet-beta.solana.com"},
		},
		[]Token{{"USDC", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}, {"USDT", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}}},
	{"devnet", "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG",
		[]Upstream{{RPC: "https://api.devnet.solana.com"}},
		[]Token{{"USDC", "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU"}}},
	{"testnet", "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY",
		[]Upstream{{RPC: "https://api.testnet.solana.com"}, {RPC: "https://solana-testnet-rpc.publicnode.com"}},
		nil},
}

// aliases are other names for built-in networks (the CLI's -u values).
var aliases = map[string]string{"mainnet-beta": "mainnet", "m": "mainnet", "d": "devnet", "t": "testnet"}

// reserved names can't be network names: they are paths at the gateway root.
var reserved = map[string]bool{"health": true, "all": true, "custom": true, "ws": true}

var genesisMu sync.Mutex

// CheckGenesis compares an upstream's genesis hash with the network's. A
// network without one pins it.
func (n *Network) CheckGenesis(got string) error {
	genesisMu.Lock()
	defer genesisMu.Unlock()
	if n.Genesis == "" {
		n.Genesis = got
		return nil
	}
	if n.Genesis != got {
		return fmt.Errorf("serves a different cluster (genesis %.12s..., want %.12s...)", got, n.Genesis)
	}
	return nil
}

// GenesisPinned returns the network's genesis hash ("" before first use of a
// custom network).
func (n *Network) GenesisPinned() string {
	genesisMu.Lock()
	defer genesisMu.Unlock()
	return n.Genesis
}

// Registry holds the built-in networks plus custom ones.
type Registry struct {
	m map[string]*Network
}

// NewRegistry returns a registry with the built-in networks.
func NewRegistry() *Registry {
	r := &Registry{m: map[string]*Network{}}
	for i := range Networks {
		n := Networks[i]
		n.Upstreams = append([]Upstream(nil), n.Upstreams...)
		r.m[n.Name] = &n
	}
	return r
}

// ValidName reports whether s can name a network in gateway paths.
func ValidName(s string) error {
	if s == "" || len(s) > 40 || strings.Trim(s, "abcdefghijklmnopqrstuvwxyz0123456789-_") != "" {
		return fmt.Errorf("network name %q: use lowercase letters, digits, - and _", s)
	}
	if reserved[s] || aliases[s] != "" {
		return fmt.Errorf("network name %q is reserved", s)
	}
	return nil
}

// CheckURL accepts only URLs with the given scheme (https or wss) and no
// credentials, query or fragment.
func CheckURL(s, scheme string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if u.Scheme != scheme || u.Host == "" {
		return fmt.Errorf("%s: only %s upstreams are allowed here", s, scheme)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("%s: no credentials, query or fragment in upstream URLs", s)
	}
	return nil
}

// ParseUpstream parses "https://rpc[+wss://ws]".
func ParseUpstream(s string) (Upstream, error) {
	rpc, ws, _ := strings.Cut(strings.TrimSpace(s), "+")
	u := Upstream{RPC: strings.TrimRight(rpc, "/"), WS: strings.TrimRight(ws, "/")}
	if err := CheckURL(u.RPC, "https"); err != nil {
		return u, err
	}
	if err := CheckURL(u.WSURL(), "wss"); err != nil {
		return u, err
	}
	return u, nil
}

// Add sets the upstreams of a network, creating it if new. Custom upstreams
// replace the built-in list (so --add mainnet=https://my-node works too). A
// new network has no genesis yet: the first upstream that answers pins it.
func (r *Registry) Add(name string, specs []string) error {
	name = strings.ToLower(strings.TrimSpace(name))
	if a, ok := aliases[name]; ok {
		name = a
	}
	if _, builtin := r.m[name]; !builtin {
		if err := ValidName(name); err != nil {
			return err
		}
	}
	var ups []Upstream
	for _, s := range specs {
		if strings.TrimSpace(s) == "" {
			continue
		}
		u, err := ParseUpstream(s)
		if err != nil {
			return err
		}
		ups = append(ups, u)
	}
	if len(ups) == 0 {
		return fmt.Errorf("network %q: no upstream URL", name)
	}
	n, ok := r.m[name]
	if !ok {
		n = &Network{Name: name}
		r.m[name] = n
	}
	n.Upstreams = ups
	return nil
}

// ParseAdd parses "name=https://a[+wss://a-ws]|https://b" into the registry.
func (r *Registry) ParseAdd(spec string) error {
	n, u, ok := strings.Cut(spec, "=")
	if !ok {
		return fmt.Errorf("want name=https://url[|https://url2], got %q", spec)
	}
	return r.Add(n, strings.Split(u, "|"))
}

// Get finds a network by name or alias (case-insensitive).
func (r *Registry) Get(name string) (*Network, bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	if a, ok := aliases[name]; ok {
		name = a
	}
	n, ok := r.m[name]
	return n, ok
}

// All returns the networks, built-ins first, then custom ones by name.
func (r *Registry) All() []*Network {
	var out []*Network
	seen := map[string]bool{}
	for _, b := range Networks {
		out = append(out, r.m[b.Name])
		seen[b.Name] = true
	}
	var extra []string
	for n := range r.m {
		if !seen[n] {
			extra = append(extra, n)
		}
	}
	sort.Strings(extra)
	for _, n := range extra {
		out = append(out, r.m[n])
	}
	return out
}
