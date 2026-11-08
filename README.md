# solanamix

Solana JSON-RPC **and PubSub websockets** through the [Nym mixnet](https://nym.com), for mainnet-beta, devnet and
testnet, or any RPC node you add. A local gateway for the Solana CLI, @solana/web3.js, wallets and curl, plus a small
CLI. One Go binary, no dependencies outside the standard library, MIT.

```
solanamix start                             # nym-socks5-client on a random active exit gateway + the gateway, in the background
solana config set --url http://127.0.0.1:8899     # or any client: RPC on :8899, PubSub on ws://127.0.0.1:8900
solanamix slot --network devnet             # current slot and block height
solanamix balance 9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM     # SOL, USDC/USDT and other SPL tokens
solanamix send-raw tx.b64                   # forward a signed transaction; solanamix holds no keys
solanamix watch slot                        # PubSub subscription over the mixnet
solanamix ip                                # the IP the RPC nodes see (a Nym exit gateway)
solanamix stop
```

## Why

Every Solana wallet, bot and CLI talks to an RPC provider, and almost every call names your accounts: `getBalance`,
`getTokenAccountsByOwner`, `getSignaturesForAddress`, an `accountSubscribe` on your address, and finally the
`sendTransaction` that carries your signed transfer. The provider sees all of that next to your IP, so it learns which
IP holds which accounts and which IP sent which transaction, before the transaction is even on chain.

Solana clients take an RPC URL, but few have a proxy setting, and the ones that confirm transactions also open a
websocket to the same host (web3.js derives `ws://host:port+1` from your URL). A local URL that sends both over the
mixnet fixes that without changing the client.

## Use

```
export SOLANAMIX_NYM_SOCKS5=/path/to/nym-socks5-client    # official Nym release binary
go install github.com/apeirs/solanamix/cmd/solanamix@latest
solanamix start                # first time: picks a random exit gateway (SOCKS5 127.0.0.1:1095), then starts both
```

`solanamix start` is `solanamix nym setup` (or `nym start`) followed by `solanamix serve` in the background; `solanamix serve` runs
the gateway in the foreground instead. The gateway serves every network on one port:

| Network | JSON-RPC | PubSub | Upstreams (tried in order) |
|---|---|---|---|
| Mainnet-beta (the default) | `http://127.0.0.1:8899` or `/mainnet`, `/mainnet-beta` | `ws://127.0.0.1:8900` (same paths) | solana-rpc.publicnode.com, public.rpc.solanavibestation.com, api.mainnet-beta.solana.com |
| Devnet | `http://127.0.0.1:8899/devnet` | `ws://127.0.0.1:8900/devnet` | api.devnet.solana.com |
| Testnet | `http://127.0.0.1:8899/testnet` | `ws://127.0.0.1:8900/testnet` | api.testnet.solana.com, solana-testnet-rpc.publicnode.com |
| Your own node or cluster | `/<name>` after `--add name=https://rpc[+wss://ws]\|https://rpc2` | same | yours |

The PubSub port is the RPC port plus one because that is how the Solana CLI and web3.js derive it from the RPC URL; the
RPC port accepts websockets too. `POST` with a JSON-RPC body (single or batch) and `GET /health` are passed through byte
for byte; nothing else on an upstream host is reachable.

Put the URL into your client:

- **@solana/web3.js** (tested with 1.98.4): `new Connection('http://127.0.0.1:8899/devnet', 'confirmed')`. Genesis
  hash, slot, balances, parsed account info, epoch info, `onSlotChange` and `onAccountChange` subscriptions, and a
  transfer built and signed locally and sent with `sendRawTransaction` all went through the gateway
  (`examples/web3-client.mjs`). A preload hook (`examples/no-bypass-hook.cjs`) recorded every TCP connect and DNS lookup
  of the Node process: the only ones were `127.0.0.1:8899` and `127.0.0.1:8900`.
- **Solana CLI**: `solana config set --url http://127.0.0.1:8899` (or `-u http://127.0.0.1:8899/devnet`). The CLI
  derives `ws://127.0.0.1:8900` the same way. I have not run the CLI against it yet. Note the TPU limit below for
  `solana program deploy`.
- **Wallets with a custom RPC setting** (Backpack, Solflare and others): enter the gateway URL. Requests from browser
  extensions are allowed without a flag. Not tested yet.
- **curl**: `curl http://127.0.0.1:8899/devnet -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}'`. curl's default form
  content type is sent upstream as JSON, so the usual examples work as they are.

Watch the gateway log when you wire up a new app. It prints one line per request or websocket message with the
network, the JSON-RPC method (never the params), the upstream and the timing. Anything that doesn't show up there did
not go through Nym.

solanamix forwards; it never signs anything. `solanamix send-raw` takes a serialized transaction as base64 (what web3.js
`serialize().toString('base64')` gives) or base58, from an argument, a file or `-`. It parses the wire format itself
(legacy and v0 messages) and refuses, before anything is sent: a transaction with an empty signature slot (unsigned),
a signature that doesn't verify (ed25519) for its account key (the message was changed after signing), a signature
count that doesn't match the message header, trailing bytes, and anything over the 1232-byte packet limit. Then it
calls `sendTransaction` and prints the signature, or the node's error with the simulation logs.

## How it uses the mixnet

- It drives the official `nym-socks5-client` and does no mixing itself. `solanamix start` / `nym setup` picks a random
  active exit gateway from the Nym API (one that runs an exit-policy network requester), with reply SURBs on, so the
  exit doesn't learn your mixnet address.
- Each upstream connection is a SOCKS5 stream through the entry gateway, three mix layers and the exit, to port 443.
  TLS runs end to end with the RPC node, so the exit only relays ciphertext. HTTP connections are kept alive, so the
  calls after the first one don't pay a new stream and TLS handshake.
- **PubSub**: for each local websocket, solanamix opens its own `wss://` socket to the upstream through the mixnet (TLS
  end to end, its own handshake with only `Host`, the websocket headers and a fixed `User-Agent`), and only then
  answers the local handshake. No extension or subprotocol is negotiated on either side, so frames are relayed byte
  for byte. The log names each subscription method, never its params.
- Node hostnames are resolved by DNS-over-HTTPS *through the mixnet*, then connected by IPv4. The local resolver never
  learns which RPC node you use.
- **Genesis guard.** Before an upstream gets its first request or websocket, solanamix asks it for `getGenesisHash` and
  compares it with the cluster's. An upstream of another cluster (a devnet URL under mainnet, a typo in `--add`, a
  provider that changed what it serves) is skipped and the next one is tried; your requests never reach it.
  Built-in networks have their genesis hash pinned in the code. A network you add pins the genesis of its first
  upstream, and every other upstream of it has to match.
- The gateway can only reach the configured upstreams (https and wss only, no credentials or query in the URL), and
  only the RPC URL itself and `/health`. The first path segment picks the network; anything else is 404.
- There is no clearnet fallback. With the mixnet client down, the gateway answers 502 with a JSON-RPC error
  (`"data":{"reason":"MixnetUnavailable"}`, which web3.js shows), websockets get 502 instead of a handshake, and the
  CLI refuses to run.
- Failover: a connection error, a timeout, a failed genesis check, HTTP 5xx, 429, 401/403 (a public endpoint refusing
  the exit, or refusing one call) or a redirect moves on to the next upstream. Redirects are never followed or passed
  back.
- Nothing from the local caller is forwarded except the method and body. Upstream sees `Content-Type`, `Accept` and a
  fixed `User-Agent`: no `Origin`, `Cookie`, `Referer`, `solana-client` header or the wallet's user agent. Only
  `Content-Type` comes back.
- The gateway listens on loopback and rejects a foreign `Host` header (DNS-rebinding guard). It also rejects any
  web-page `Origin` (HTTP and websocket), so a site you visit can't use your gateway. Browser extensions and
  non-browser clients are allowed; `--allow-origin` adds pages.
- Logs name the network, the JSON-RPC method or batch (`batch of 3 (getSlot x1, getBlockHeight x1, getBalance x1)`),
  the upstream host and the timing. Pubkeys, signatures, transactions and bodies are never logged, not even in error
  messages, and neither is the path of an upstream URL (where some providers put an API key).

## Limits (read these)

- **It hides your IP, not your accounts.** The RPC node still sees which accounts you query and which transactions
  you send, and everything on chain stays public. solanamix removes the IP link, not the on-chain one.
- **One exit IP, one session.** All requests leave through the same exit gateway, so a node can link the accounts one
  wallet asks about to each other. For unlinkable sessions, run separate client identities
  (`solanamix nym setup --id other --port 1096`) and separate gateways.
- **Shared rate limits and blocks.** Public endpoints limit per IP, and the exit IP is shared with other Nym users. On
  2026-11-08 `api.mainnet-beta.solana.com` answered solanamix through a Nym exit with 403 "Your IP or provider is blocked
  from this endpoint" (curl through the same exit got 200), and publicnode refuses indexed calls such as
  `getTokenAccountsByOwner` without a token. Both cases fail over to the next upstream; that's why mainnet lists three.
  Devnet has one public upstream, and its RPC faucet is closed (`requestAirdrop` answers 410).
- **Latency is seconds.** In my live run a CLI command on a fresh mixnet stream took 6 to 11 s (DoH, TLS and the
  genesis check included). Through the gateway the first request per network took up to 12 s, later ones 0.6 to 3 s
  on the kept-alive stream; a PubSub websocket opened in 2 to 6 s. A blockhash is valid for about a minute, which leaves room
  for a send. Fine for a wallet; slow for a trading bot.
- **TPU sends bypass any RPC URL.** `solana program deploy` and some bots send transactions straight to the leaders'
  TPU ports over QUIC/UDP, not through RPC. That traffic doesn't go through solanamix (and UDP doesn't fit the SOCKS5
  exit). Use `solana program deploy --use-rpc`, or check with the gateway log.
- **Clients that ignore your URL bypass it.** solanamix only protects requests that reach it. A wallet's price feeds,
  token lists, NFT images and analytics are not covered. Check with the gateway log, or with the preload hook for Node
  apps.
- **A rejected transaction can be linked to its retry.** Re-sending the same signed bytes from a different exit still
  has the same signature.
- Not tested yet: the Solana CLI itself, wallet UIs, `getProgramAccounts`-heavy apps.

## Related

[kepler-ops-maker/solmix](https://github.com/kepler-ops-maker/solmix) (posted to the Nym forum as t/2667) is an earlier,
separate Python project: a JSON-RPC gateway and CLI over nym-socks5-client. This one is a different code base (Go, standard library
only) and adds the PubSub websocket path that wallets and web3.js use to confirm transactions, the genesis guard,
local signature checks before a send, and the Host/Origin guards.

## Tests

`go vet ./... && go test ./...` runs offline against a fake mixnet: a recording SOCKS5 proxy, a DoH server and fake TLS
Solana nodes with JSON-RPC and PubSub. The tests check that:

- every connection goes through the proxy, and the only name lookups are DoH over it;
- JSON-RPC bodies (single and batch) pass through byte for byte, after the upstream's genesis check, and `/health` too;
- PubSub websockets are relayed frame for frame after the genesis check, with only solanamix's own handshake headers
  upstream (no Origin, cookies, extensions or subprotocols), and the CLI's own websocket path works;
- an upstream of another cluster is skipped before it sees a request or a websocket, and a custom network pins the
  genesis of its first upstream;
- `/<network>` routes work and nothing but the RPC path and `/health` gets through; methods and body types are
  checked;
- request and response headers are stripped;
- the Host and Origin guards hold, for HTTP and websockets;
- with no proxy there is a 502 and no fallback, for HTTP and websockets, in both DNS modes, even when a direct
  connection to the upstream would work;
- failover covers connection errors, timeouts, 5xx, 429, 403 (at the genesis check and after it) and redirects (which
  are never followed), and only https/wss upstreams are accepted;
- no pubkey, signature, transaction, body or key-bearing upstream path reaches a log line or an error answer, on
  success or failure;
- base58, the transaction parser and signature checks (legacy and v0; unsigned, edited, wrong signer, wrong count,
  trailing bytes, oversize), the websocket handshake (RFC 6455 vector, wrong accept key, forced extension) and frame
  codec, and the log labels match known vectors.

I also broke each guard on purpose (51 mutations: Host and Origin guards, path routing, methods, the genesis check on
the RPC and websocket paths and its pinning, https/wss-only upstreams, a planted clearnet fallback in the dialer, the
HTTP client and the websocket dialer, local DNS, redirects, failover, timeouts, log redaction, header stripping, body
types, the websocket handshake checks and masking, and every `send-raw` refusal) and checked that a test fails every
time.

The live run against public RPC nodes through the real Nym mixnet is in `examples/`: `live-2026-11-08.txt` (start,
CLI on all three clusters, `send-raw` refusals and the node's verdict, the gateway with curl, guards, the no-fallback
check), `live-web3.txt` (web3.js with the preload hook, including PubSub subscriptions and a transfer signed by a
fresh, never-funded devnet key: the node rejected it with "Attempt to debit an account but found no record of a
prior credit"), `live-gateway.log` and `nym-client-proxy-lines.txt`.
These logs were recorded before the project was renamed from solmix to solanamix, so they show the old binary name.
