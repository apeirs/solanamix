package solanamix_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apeirs/solanamix/gateway"
	"github.com/apeirs/solanamix/internal/fakes"
	"github.com/apeirs/solanamix/mixnet"
	"github.com/apeirs/solanamix/sol"
	"github.com/apeirs/solanamix/wsx"
)

const (
	mainGenesis = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	devGenesis  = "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG"
	testPubkey  = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
	testSig     = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW"
	testTx      = "AT9vRhrlH3wjnLyRd6ZxWYZPdx5XyK0MVjVkWmEn0JVQ8Q0bX2D0x9Ylh8YqW0KHAAEAAQPbM6dlMkYZn4a+sBK3ARQRbbIfY="
)

// seen is one request as a fake upstream received it. A websocket message
// is recorded with Method "WS".
type seen struct {
	Host, Method, Path, Body string
	Header                   http.Header
}

type world struct {
	socks *fakes.SOCKS
	d     *mixnet.Dialer
	cl    *sol.Client
	mu    sync.Mutex
	got   []seen
	logs  []string
}

func (w *world) add(s seen) {
	w.mu.Lock()
	w.got = append(w.got, s)
	w.mu.Unlock()
}

func hostOf(r *http.Request) string {
	host, _, _ := net.SplitHostPort(r.Host)
	if host == "" {
		host = r.Host
	}
	return host
}

func (w *world) record(r *http.Request) seen {
	b, _ := io.ReadAll(r.Body)
	s := seen{hostOf(r), r.Method, r.URL.Path, string(b), r.Header.Clone()}
	w.add(s)
	return s
}

func (w *world) last(host string) seen {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.got) - 1; i >= 0; i-- {
		if w.got[i].Host == host {
			return w.got[i]
		}
	}
	return seen{}
}

// calls lists what host was asked, in order: the JSON-RPC method (from the
// body), "GET /health", "UPGRADE" (a websocket handshake) or "WS <method>".
func (w *world) calls(host string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, s := range w.got {
		if s.Host != host {
			continue
		}
		switch s.Method {
		case "POST":
			out = append(out, sol.Label([]byte(s.Body)))
		case "UPGRADE":
			out = append(out, "UPGRADE")
		case "WS":
			out = append(out, "WS "+sol.Label([]byte(s.Body)))
		default:
			out = append(out, s.Method+" "+s.Path)
		}
	}
	return out
}

func (w *world) logf(f string, a ...any) {
	w.mu.Lock()
	w.logs = append(w.logs, fmt.Sprintf(f, a...))
	w.mu.Unlock()
}

func (w *world) allLogs() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Join(w.logs, "\n")
}

// pubsub is a fake PubSub server: it answers each subscription with id 7
// and one notification.
func (w *world) pubsub(rw http.ResponseWriter, r *http.Request) {
	w.add(seen{hostOf(r), "UPGRADE", r.URL.Path, "", r.Header.Clone()})
	key, err := wsx.CheckClient(r)
	if err != nil {
		http.Error(rw, err.Error(), 400)
		return
	}
	conn, brw, _ := rw.(http.Hijacker).Hijack()
	defer conn.Close()
	io.WriteString(conn, wsx.AcceptResponse(key))
	for {
		f, err := wsx.ReadFrame(brw)
		if err != nil || f.Op == wsx.OpClose {
			return
		}
		if !f.Masked {
			return // a server must close on unmasked client frames
		}
		w.add(seen{hostOf(r), "WS", r.URL.Path, string(f.Payload), nil})
		wsx.WriteFrame(conn, wsx.OpText, []byte(`{"jsonrpc":"2.0","result":7,"id":1}`), false)
		wsx.WriteFrame(conn, wsx.OpText, []byte(`{"jsonrpc":"2.0","method":"slotNotification","params":{"result":{"slot":321,"parent":320,"root":300},"subscription":7}}`), false)
	}
}

// node is a fake Solana RPC node (JSON-RPC, /health, PubSub) of the cluster
// with genesis g. late503 makes it pass the genesis check and then fail
// everything else.
func (w *world) node(g string, late503 bool) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if wsx.IsUpgrade(r) {
			if late503 {
				w.add(seen{hostOf(r), "UPGRADE", r.URL.Path, "", r.Header.Clone()})
				http.Error(rw, "overloaded", 503)
				return
			}
			w.pubsub(rw, r)
			return
		}
		s := w.record(r)
		rw.Header().Set("Set-Cookie", "track=1")
		rw.Header().Set("X-Upstream-Secret", "1")
		rw.Header().Set("Content-Type", "application/json; charset=utf-8")
		if s.Method == "GET" && s.Path == "/health" {
			io.WriteString(rw, "ok")
			return
		}
		var req struct {
			Method string `json:"method"`
		}
		json.Unmarshal([]byte(s.Body), &req)
		if req.Method == "getGenesisHash" {
			fmt.Fprintf(rw, `{"jsonrpc":"2.0","result":"%s","id":1}`, g)
			return
		}
		if late503 {
			http.Error(rw, "upstream overloaded", 503)
			return
		}
		if hostOf(r) == "indexed.example" && req.Method == "getTokenAccountsByOwner" {
			rw.WriteHeader(403)
			io.WriteString(rw, `{"jsonrpc":"2.0","error":{"code":403,"message":"Indexed requests require a personal token"},"id":1}`)
			return
		}
		switch req.Method {
		case "getSlot":
			io.WriteString(rw, `{"jsonrpc":"2.0","result":466812285,"id":1}`)
		case "getBalance":
			io.WriteString(rw, `{"jsonrpc":"2.0","result":{"context":{"slot":466812285},"value":1500000000},"id":1}`)
		case "sendTransaction":
			io.WriteString(rw, `{"jsonrpc":"2.0","error":{"code":-32002,"message":"Transaction simulation failed: Attempt to debit an account but found no record of a prior credit.","data":{"logs":[]}},"id":1}`)
		case "redirect":
			rw.Header().Set("Location", "http://plain.example/")
			rw.WriteHeader(302)
		default:
			io.WriteString(rw, `{"jsonrpc":"2.0","result":"ok","id":1}`)
		}
	}
}

// The fake world. api.mainnet-beta.solana.com and other.example are good
// mainnet nodes, api.devnet.solana.com is devnet, wrong.example is a devnet
// node posing as mainnet, bad.example answers 500, busy.example 429,
// blocked.example 403 ("Your IP or provider is blocked"), slow.example never
// answers, late.example passes the genesis check and then answers 503,
// drop.example passes it and then drops the connection, and redir.example
// redirects everything. All are TLS, reachable only through the fake SOCKS
// proxy, with names known only to the fake DoH resolver.
func newWorld(t *testing.T) *world {
	t.Helper()
	ca := fakes.NewCA()
	w := &world{}
	serve := func(h http.HandlerFunc, name string) string {
		ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{ca.Leaf(name)}, NextProtos: []string{"http/1.1"}})
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: h}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
		return ln.Addr().String()
	}
	good := serve(w.node(mainGenesis, false), "api.mainnet-beta.solana.com")
	other := serve(w.node(mainGenesis, false), "other.example")
	indexed := serve(w.node(mainGenesis, false), "indexed.example")
	dev := serve(w.node(devGenesis, false), "api.devnet.solana.com")
	wrong := serve(w.node(devGenesis, false), "wrong.example")
	late := serve(w.node(mainGenesis, true), "late.example")
	bad := serve(func(rw http.ResponseWriter, r *http.Request) { w.record(r); http.Error(rw, "boom", 500) }, "bad.example")
	busy := serve(func(rw http.ResponseWriter, r *http.Request) {
		w.record(r)
		http.Error(rw, `{"jsonrpc":"2.0","error":{"code":429,"message":"Too many requests for a specific RPC call"},"id":1}`, 429)
	}, "busy.example")
	blocked := serve(func(rw http.ResponseWriter, r *http.Request) {
		w.record(r)
		rw.WriteHeader(403)
		io.WriteString(rw, `{"jsonrpc":"2.0","error":{"code": 403,"message":"Your IP or provider is blocked from this endpoint"}, "id": 1 }`)
	}, "blocked.example")
	slow := serve(func(rw http.ResponseWriter, r *http.Request) {
		w.record(r)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}, "slow.example")
	drop := serve(func(rw http.ResponseWriter, r *http.Request) {
		s := w.record(r)
		if strings.Contains(s.Body, "getGenesisHash") {
			fmt.Fprintf(rw, `{"jsonrpc":"2.0","result":"%s","id":1}`, mainGenesis)
			return
		}
		conn, _, _ := rw.(http.Hijacker).Hijack()
		conn.Close()
	}, "drop.example")
	redir := serve(func(rw http.ResponseWriter, r *http.Request) {
		w.record(r)
		http.Redirect(rw, r, "https://other.example/", http.StatusTemporaryRedirect)
	}, "redir.example")
	doh := fakes.DoH(ca, "1.1.1.1", map[string][]string{
		"api.mainnet-beta.solana.com": {"192.0.2.9"}, "other.example": {"192.0.2.10"}, "api.devnet.solana.com": {"192.0.2.11"},
		"wrong.example": {"192.0.2.12"}, "late.example": {"192.0.2.13"}, "bad.example": {"192.0.2.8"},
		"busy.example": {"192.0.2.7"}, "slow.example": {"192.0.2.6"}, "redir.example": {"192.0.2.14"}, "drop.example": {"192.0.2.15"},
		"blocked.example": {"192.0.2.16"}, "indexed.example": {"192.0.2.17"}})
	w.socks = fakes.NewSOCKS(map[string]string{"1.1.1.1:443": doh, "192.0.2.9:443": good, "192.0.2.10:443": other,
		"192.0.2.11:443": dev, "192.0.2.12:443": wrong, "192.0.2.13:443": late, "192.0.2.8:443": bad,
		"192.0.2.7:443": busy, "192.0.2.6:443": slow, "192.0.2.14:443": redir, "192.0.2.15:443": drop, "192.0.2.16:443": blocked, "192.0.2.17:443": indexed})
	t.Cleanup(w.socks.Close)
	w.d = &mixnet.Dialer{Proxy: w.socks.Addr, DNS: mixnet.DNSDoH, RootCAs: ca.Pool, Resolvers: []string{"1.1.1.1"}, Timeout: 5 * time.Second}
	w.cl = &sol.Client{D: w.d, Timeout: 10 * time.Second, Log: w.logf}
	return w
}

func ups(urls ...string) []sol.Upstream {
	var out []sol.Upstream
	for _, u := range urls {
		out = append(out, sol.Upstream{RPC: "https://" + u})
	}
	return out
}

// registry: mainnet = bad (500), then api.mainnet-beta.solana.com, then
// other.example; devnet = busy (429), then api.devnet.solana.com; private =
// other.example.
func (w *world) registry(t *testing.T) *sol.Registry {
	reg := sol.NewRegistry()
	m, _ := reg.Get("mainnet")
	m.Upstreams = ups("bad.example", "api.mainnet-beta.solana.com", "other.example")
	d, _ := reg.Get("devnet")
	d.Upstreams = ups("busy.example", "api.devnet.solana.com")
	if err := reg.Add("private", []string{"https://other.example"}); err != nil {
		t.Fatal(err)
	}
	return reg
}

func (w *world) gateway(t *testing.T, def string) *httptest.Server {
	g := &gateway.Gateway{Client: w.cl, Nets: w.registry(t), Default: def, Log: w.logf}
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url string, hdr map[string]string, body string) (*http.Response, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, url, rd)
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

// wsDial opens a websocket to the gateway like web3.js does (extra headers
// included) and returns the connection and the handshake status.
func wsDial(t *testing.T, srvURL, path string, hdr map[string]string) (net.Conn, *bufio.Reader, int) {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(srvURL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(srvURL, "http://")
	if h, ok := hdr["Host"]; ok {
		host = h
	}
	req := "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
	for k, v := range hdr {
		if k != "Host" {
			req += k + ": " + v + "\r\n"
		}
	}
	conn.SetDeadline(time.Now().Add(20 * time.Second))
	io.WriteString(conn, req+"\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 101 {
		resp.Body.Close()
		conn.Close()
		return nil, nil, resp.StatusCode
	}
	if resp.Header.Get("Sec-WebSocket-Accept") != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" || resp.Header.Get("Sec-WebSocket-Extensions") != "" {
		t.Fatalf("bad 101: %v", resp.Header)
	}
	return conn, br, 101
}

func wsRoundTrip(t *testing.T, conn net.Conn, br *bufio.Reader, msg string) []string {
	t.Helper()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := wsx.WriteFrame(conn, wsx.OpText, []byte(msg), true); err != nil {
		t.Fatal(err)
	}
	var out []string
	for i := 0; i < 2; i++ {
		f, err := wsx.ReadFrame(br)
		if err != nil {
			t.Fatal(err)
		}
		if f.Masked {
			t.Fatal("server frame masked")
		}
		out = append(out, string(f.Payload))
	}
	return out
}

// assertOnlyMixnet: every connection went through the SOCKS proxy, by IPv4
// literal after DoH over it; no hostname was resolved locally or handed to
// the exit (the fake names don't exist anywhere else).
func assertOnlyMixnet(t *testing.T, w *world, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, r := range w.socks.Seen() {
		got[r.Host] = true
		if r.ATYP != 1 {
			t.Errorf("SOCKS CONNECT %v: DoH mode must connect by IPv4 literal", r)
		}
	}
	for _, h := range append(want, "1.1.1.1") {
		if !got[h] {
			t.Errorf("SOCKS never saw %s (saw %v)", h, got)
		}
	}
}

// JSON-RPC requests pass through untouched, byte for byte, after the genesis
// check. The first upstream answers 500, the second one serves.
func TestPassThroughOverMixnet(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, "mainnet")
	body := `{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["` + testPubkey + `",{"commitment":"confirmed"}]}`
	resp, got := do(t, "POST", srv.URL, map[string]string{"Content-Type": "application/json"}, body)
	if resp.StatusCode != 200 || !strings.Contains(got, `"value":1500000000`) {
		t.Fatalf("getBalance: %d %s", resp.StatusCode, got)
	}
	s := w.last("api.mainnet-beta.solana.com")
	if s.Body != body || s.Method != "POST" || s.Path != "/" || s.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("upstream got %s %s %q %q", s.Method, s.Path, s.Body, s.Header.Get("Content-Type"))
	}
	if c := w.calls("api.mainnet-beta.solana.com"); len(c) != 2 || c[0] != "getGenesisHash" || c[1] != "getBalance" {
		t.Fatalf("upstream saw %v, want the genesis check, then the request", c)
	}
	if len(w.calls("bad.example")) == 0 {
		t.Fatal("first upstream was never tried")
	}
	// A batch, as web3.js sends for several calls at once.
	batch := `[{"jsonrpc":"2.0","id":1,"method":"getSlot"},{"jsonrpc":"2.0","id":2,"method":"getBalance","params":["` + testPubkey + `"]}]`
	if resp, _ = do(t, "POST", srv.URL+"/", nil, batch); resp.StatusCode != 200 || w.last("api.mainnet-beta.solana.com").Body != batch {
		t.Fatalf("batch: %d", resp.StatusCode)
	}
	// The node's own JSON-RPC error comes back as it is.
	send := `{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["` + testTx + `",{"encoding":"base64"}]}`
	resp, got = do(t, "POST", srv.URL, map[string]string{"Content-Type": "application/json; charset=utf-8"}, send)
	if resp.StatusCode != 200 || !strings.Contains(got, "no record of a prior credit") || w.last("api.mainnet-beta.solana.com").Body != send {
		t.Fatalf("sendTransaction: %d %s", resp.StatusCode, got)
	}
	// text/plain and curl's default form type go up as JSON, unchanged.
	for _, ct := range []string{"text/plain", "application/x-www-form-urlencoded"} {
		resp, _ = do(t, "POST", srv.URL, map[string]string{"Content-Type": ct}, body)
		if s := w.last("api.mainnet-beta.solana.com"); resp.StatusCode != 200 || s.Body != body || s.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("%s: %d %q %q", ct, resp.StatusCode, s.Body, s.Header.Get("Content-Type"))
		}
	}
	// GET /health passes through.
	if resp, got = do(t, "GET", srv.URL+"/health", nil, ""); resp.StatusCode != 200 || got != "ok" || w.last("api.mainnet-beta.solana.com").Path != "/health" {
		t.Fatalf("health: %d %q", resp.StatusCode, got)
	}
	// An unsupported body type is refused before anything goes out.
	before := len(w.socks.Seen())
	for _, ct := range []string{"multipart/form-data; boundary=x", "application/octet-stream", "image/png"} {
		if resp, _ = do(t, "POST", srv.URL, map[string]string{"Content-Type": ct}, "x=1"); resp.StatusCode != 415 {
			t.Fatalf("%s: %d", ct, resp.StatusCode)
		}
	}
	if len(w.socks.Seen()) != before {
		t.Fatal("refused body still opened a stream")
	}
	assertOnlyMixnet(t, w, "192.0.2.8", "192.0.2.9")
}

// /devnet, /mainnet, /mainnet-beta and custom networks on one port, a
// listener with devnet at the root, and nothing but the RPC path and /health.
func TestNetworkRoutes(t *testing.T) {
	w := newWorld(t)
	main := w.gateway(t, "mainnet")
	devRoot := w.gateway(t, "devnet")
	for _, c := range []struct{ url, host string }{
		{main.URL, "api.mainnet-beta.solana.com"},
		{main.URL + "/mainnet", "api.mainnet-beta.solana.com"},
		{main.URL + "/mainnet-beta/", "api.mainnet-beta.solana.com"},
		{main.URL + "/devnet", "api.devnet.solana.com"},
		{devRoot.URL + "/", "api.devnet.solana.com"},
		{main.URL + "/private", "other.example"},
	} {
		resp, body := do(t, "POST", c.url, nil, `{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)
		if resp.StatusCode != 200 || !strings.Contains(body, "466812285") || w.last(c.host).Path != "/" {
			t.Fatalf("%s: %d %s (upstream %s saw %q)", c.url, resp.StatusCode, body, c.host, w.last(c.host).Path)
		}
	}
	if len(w.calls("busy.example")) == 0 {
		t.Fatal("devnet 429 upstream never tried")
	}
	if resp, body := do(t, "GET", main.URL+"/devnet/health", nil, ""); resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("/devnet/health: %d %s", resp.StatusCode, body)
	}
	if _, body := do(t, "GET", main.URL+"/", nil, ""); !strings.Contains(body, "/devnet") {
		t.Fatalf("help: %s", body)
	}
	n := len(w.got)
	for _, p := range []string{"/%2e%2e", "/devnet/%2e%2e/health", "/admin", "/devnet/x", "/devnet/health/x", "//devnet",
		"/devnet//", "/a/b/c", "/unknownnet", "/health/health", "/devnet/../admin", "/ws"} {
		if resp, _ := do(t, "POST", main.URL+p, nil, `{"method":"getSlot"}`); resp.StatusCode != 404 {
			t.Fatalf("%s: want 404, got %d", p, resp.StatusCode)
		}
	}
	if len(w.got) != n {
		t.Fatalf("a refused path was forwarded: %v", w.got[n:])
	}
	for _, c := range []struct{ method, path string }{{"PUT", "/"}, {"DELETE", "/devnet"}, {"POST", "/health"}, {"PATCH", "/devnet/health"}} {
		if resp, _ := do(t, c.method, main.URL+c.path, nil, "{}"); resp.StatusCode != 405 {
			t.Fatalf("%s %s: %d", c.method, c.path, resp.StatusCode)
		}
	}
	if len(w.got) != n {
		t.Fatalf("a refused method was forwarded: %v", w.got[n:])
	}
}

// An upstream of another cluster is skipped after the genesis check and
// never sees the real request; a custom network pins the first genesis.
func TestGenesisGuard(t *testing.T) {
	w := newWorld(t)
	reg := w.registry(t)
	m, _ := reg.Get("mainnet")
	m.Upstreams = ups("wrong.example", "other.example")
	srv := httptest.NewServer(&gateway.Gateway{Client: w.cl, Nets: reg, Default: "mainnet", Log: w.logf})
	defer srv.Close()
	body := `{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["` + testPubkey + `"]}`
	resp, _ := do(t, "POST", srv.URL, nil, body)
	if resp.StatusCode != 200 || w.last("other.example").Body != body {
		t.Fatalf("getBalance: %d", resp.StatusCode)
	}
	if c := w.calls("wrong.example"); len(c) != 1 || c[0] != "getGenesisHash" {
		t.Fatalf("wrong-cluster upstream saw %v, want only the genesis check", c)
	}
	if !strings.Contains(w.allLogs(), "serves a different cluster") {
		t.Fatalf("wrong cluster not logged:\n%s", w.allLogs())
	}
	// Each upstream is checked once per network; a failed one is re-checked, never trusted.
	do(t, "POST", srv.URL, nil, body)
	if c := w.calls("wrong.example"); len(c) != 2 {
		t.Fatalf("wrong.example: %v", c)
	}
	if c := strings.Count(strings.Join(w.calls("other.example"), " "), "getGenesisHash"); c != 1 {
		t.Fatalf("other.example genesis-checked %d times", c)
	}
	// Only a wrong-cluster upstream left: no answer, no request, no websocket.
	m.Upstreams = ups("wrong.example")
	resp, got := do(t, "POST", srv.URL, nil, `{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["`+testTx+`"]}`)
	if resp.StatusCode != 502 || !strings.Contains(got, "different cluster") || !strings.Contains(got, "MixnetUnavailable") {
		t.Fatalf("all wrong cluster: %d %s", resp.StatusCode, got)
	}
	if _, _, status := wsDial(t, srv.URL, "/", nil); status != 502 {
		t.Fatalf("websocket to a wrong-cluster upstream: %d", status)
	}
	for _, c := range w.calls("wrong.example") {
		if c != "getGenesisHash" {
			t.Fatalf("wrong-cluster upstream got %s", c)
		}
	}
	// A custom network pins the first genesis; a node of another cluster
	// added later to it is refused.
	if err := reg.Add("mine", []string{"https://api.devnet.solana.com", "https://wrong.example", "https://other.example"}); err != nil {
		t.Fatal(err)
	}
	mine, _ := reg.Get("mine")
	var slot uint64
	if used, err := w.cl.Call(context.Background(), mine, "getSlot", nil, &slot); err != nil || used != "https://api.devnet.solana.com" || slot != 466812285 {
		t.Fatalf("pin: %v %s", err, used)
	}
	if mine.GenesisPinned() != devGenesis {
		t.Fatalf("pinned %q", mine.GenesisPinned())
	}
	mine.Upstreams = ups("other.example")
	before := len(w.calls("other.example"))
	if _, err := w.cl.Call(context.Background(), mine, "getSlot", nil, &slot); err == nil || !strings.Contains(err.Error(), "different cluster") {
		t.Fatalf("mainnet node under a devnet-pinned network: %v", err)
	}
	if c := w.calls("other.example")[before:]; len(c) != 1 || c[0] != "getGenesisHash" {
		t.Fatalf("other.example saw %v", c)
	}
}

// PubSub: the websocket is relayed frame for frame to the upstream's, through
// the mixnet, after the upstream passed the genesis check. Only our own
// handshake headers go up.
func TestPubSubOverMixnet(t *testing.T) {
	w := newWorld(t)
	reg := w.registry(t)
	m, _ := reg.Get("mainnet")
	m.Upstreams = ups("wrong.example", "late.example", "api.mainnet-beta.solana.com")
	srv := httptest.NewServer(&gateway.Gateway{Client: w.cl, Nets: reg, Default: "mainnet", Log: w.logf})
	defer srv.Close()
	conn, br, status := wsDial(t, srv.URL, "/", map[string]string{"Origin": "chrome-extension://bfnaelmomeimhlpmgjnjophhpkkoljpa",
		"User-Agent": "Phantom/24", "Cookie": "s=1", "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits", "Sec-WebSocket-Protocol": "x"})
	if status != 101 {
		t.Fatalf("handshake: %d", status)
	}
	sub := `{"jsonrpc":"2.0","id":1,"method":"accountSubscribe","params":["` + testPubkey + `",{"encoding":"jsonParsed"}]}`
	got := wsRoundTrip(t, conn, br, sub)
	if got[0] != `{"jsonrpc":"2.0","result":7,"id":1}` || !strings.Contains(got[1], `"slot":321`) {
		t.Fatalf("relayed: %v", got)
	}
	if s := w.last("api.mainnet-beta.solana.com"); s.Method != "WS" || s.Body != sub {
		t.Fatalf("upstream got %+v", s)
	}
	got = wsRoundTrip(t, conn, br, `{"jsonrpc":"2.0","id":2,"method":"slotSubscribe"}`)
	if !strings.Contains(got[1], "slotNotification") {
		t.Fatalf("second message: %v", got)
	}
	wsx.WriteFrame(conn, wsx.OpClose, []byte{3, 0xe8}, true)
	conn.Close()
	want := []string{"getGenesisHash", "UPGRADE", "WS accountSubscribe", "WS slotSubscribe"}
	if c := w.calls("api.mainnet-beta.solana.com"); strings.Join(c, ",") != strings.Join(want, ",") {
		t.Fatalf("upstream saw %v, want %v", c, want)
	}
	if c := w.calls("wrong.example"); len(c) != 1 || c[0] != "getGenesisHash" {
		t.Fatalf("wrong-cluster upstream saw %v", c)
	}
	if c := w.calls("late.example"); len(c) != 2 || c[1] != "UPGRADE" {
		t.Fatalf("late.example (503 on upgrade) saw %v", c)
	}
	var up http.Header
	for _, s := range w.got {
		if s.Method == "UPGRADE" && s.Host == "api.mainnet-beta.solana.com" {
			up = s.Header
		}
	}
	for _, k := range []string{"Origin", "Cookie", "Sec-Websocket-Extensions", "Sec-Websocket-Protocol"} {
		if up.Get(k) != "" {
			t.Errorf("%s leaked upstream: %q", k, up.Get(k))
		}
	}
	if up.Get("User-Agent") != sol.UserAgent || up.Get("Sec-Websocket-Key") == "dGhlIHNhbXBsZSBub25jZQ==" {
		t.Errorf("upstream handshake: %v", up)
	}
	time.Sleep(100 * time.Millisecond)
	logs := w.allLogs()
	for _, want := range []string{"mainnet websocket open -> mixnet -> api.mainnet-beta.solana.com", "mainnet websocket accountSubscribe", "mainnet websocket slotSubscribe", "2 messages up"} {
		if !strings.Contains(logs, want) {
			t.Errorf("log lacks %q:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, testPubkey) {
		t.Fatalf("pubkey in the log:\n%s", logs)
	}
	// /devnet on the same port, as web3.js derives it (ws://host:port+1/devnet).
	conn, br, status = wsDial(t, srv.URL, "/devnet", nil)
	if status != 101 {
		t.Fatalf("devnet handshake: %d", status)
	}
	wsRoundTrip(t, conn, br, `{"jsonrpc":"2.0","id":1,"method":"slotSubscribe"}`)
	conn.Close()
	if c := w.calls("api.devnet.solana.com"); len(c) != 3 || c[2] != "WS slotSubscribe" {
		t.Fatalf("devnet saw %v", c)
	}
	assertOnlyMixnet(t, w, "192.0.2.9", "192.0.2.11", "192.0.2.12", "192.0.2.13")
}

// The CLI's own PubSub path (solanamix watch): DialWS over the mixnet.
func TestDialWS(t *testing.T) {
	w := newWorld(t)
	reg := w.registry(t)
	d, _ := reg.Get("devnet")
	ws, err := w.cl.DialWS(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if ws.Upstream != "wss://api.devnet.solana.com" {
		t.Fatalf("upstream %s", ws.Upstream)
	}
	wsx.WriteFrame(ws, wsx.OpText, []byte(`{"jsonrpc":"2.0","id":1,"method":"slotSubscribe"}`), true)
	f, err := wsx.ReadFrame(ws.Reader)
	if err != nil || string(f.Payload) != `{"jsonrpc":"2.0","result":7,"id":1}` {
		t.Fatalf("%v %s", err, f.Payload)
	}
	if c := w.calls("busy.example"); len(c) != 1 || c[0] != "getGenesisHash" {
		t.Fatalf("busy.example (429) saw %v", c)
	}
}

func TestPubSubGuards(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, "mainnet")
	before := len(w.socks.Seen())
	for _, c := range []struct {
		path   string
		hdr    map[string]string
		status int
	}{
		{"/", map[string]string{"Host": "evil.example:8900"}, 403},
		{"/", map[string]string{"Origin": "https://evil.example"}, 403},
		{"/", map[string]string{"Origin": "null"}, 403},
		{"/unknownnet", nil, 404},
		{"/devnet/health", nil, 404},
		{"/%2e%2e", nil, 404},
	} {
		if _, _, status := wsDial(t, srv.URL, c.path, c.hdr); status != c.status {
			t.Fatalf("%s %v: want %d, got %d", c.path, c.hdr, c.status, status)
		}
	}
	// A broken handshake (no key) is refused before any stream opens.
	resp, _ := do(t, "GET", srv.URL+"/", map[string]string{"Upgrade": "websocket", "Connection": "Upgrade", "Sec-WebSocket-Version": "13"}, "")
	if resp.StatusCode != 400 {
		t.Fatalf("no key: %d", resp.StatusCode)
	}
	if n := len(w.socks.Seen()); n != before {
		t.Fatalf("refused websockets still opened %d mixnet streams", n-before)
	}
}

// Nothing about the local caller reaches the upstream, and nothing but the
// content type comes back.
func TestHeaderStripping(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, "mainnet")
	resp, _ := do(t, "POST", srv.URL, map[string]string{
		"Origin": "chrome-extension://bfnaelmomeimhlpmgjnjophhpkkoljpa", "Cookie": "session=1", "Referer": "https://dapp.example/",
		"User-Agent": "Phantom/24", "X-Forwarded-For": "192.0.2.77", "Authorization": "Bearer x", "Accept-Language": "da-DK",
		"Solana-Client": "js/1.98.4", "X-Api-Key": "callers-own-key",
	}, `{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)
	if resp.StatusCode != 200 {
		t.Fatal(resp.Status)
	}
	h := w.last("api.mainnet-beta.solana.com").Header
	for _, k := range []string{"Origin", "Cookie", "Referer", "X-Forwarded-For", "Authorization", "Accept-Language", "Solana-Client", "X-Api-Key"} {
		if h.Get(k) != "" {
			t.Errorf("%s leaked upstream: %q", k, h.Get(k))
		}
	}
	if h.Get("User-Agent") != sol.UserAgent {
		t.Errorf("User-Agent upstream = %q", h.Get("User-Agent"))
	}
	for _, k := range []string{"Set-Cookie", "Location", "X-Upstream-Secret"} {
		if resp.Header.Get(k) != "" {
			t.Errorf("upstream header %s leaked back: %v", k, resp.Header)
		}
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "chrome-extension://bfnaelmomeimhlpmgjnjophhpkkoljpa" {
		t.Errorf("extension origin not allowed: %v", resp.Header)
	}
	resp, _ = do(t, "OPTIONS", srv.URL, map[string]string{"Origin": "chrome-extension://abc",
		"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type, solana-client, bad header"}, "")
	if resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Headers") != "content-type, solana-client" {
		t.Errorf("preflight: %d %v", resp.StatusCode, resp.Header)
	}
}

func TestHostAndOriginGuard(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, "mainnet")
	before := len(w.socks.Seen())
	for _, host := range []string{"evil.example", "evil.example:8899", "127.0.0.1.evil.example"} {
		if resp, _ := do(t, "POST", srv.URL, map[string]string{"Host": host}, `{"method":"getSlot"}`); resp.StatusCode != 403 {
			t.Fatalf("rebinding Host %s accepted: %d", host, resp.StatusCode)
		}
	}
	for _, o := range []string{"https://evil.example", "null", "http://127.0.0.1:8080"} {
		if resp, _ := do(t, "POST", srv.URL, map[string]string{"Origin": o, "Content-Type": "application/json"}, `{"method":"sendTransaction","params":["`+testTx+`"]}`); resp.StatusCode != 403 {
			t.Fatalf("web page origin %s accepted: %d", o, resp.StatusCode)
		}
	}
	if resp, _ := do(t, "OPTIONS", srv.URL, map[string]string{"Origin": "https://evil.example"}, ""); resp.StatusCode != 403 {
		t.Fatalf("preflight from a web page accepted: %d", resp.StatusCode)
	}
	if n := len(w.socks.Seen()); n != before {
		t.Fatalf("refused requests still opened %d mixnet streams", n-before)
	}
	g := &gateway.Gateway{Client: w.cl, Nets: w.registry(t), Default: "mainnet", Origins: []string{"https://wallet.example"}}
	s2 := httptest.NewServer(g)
	defer s2.Close()
	if resp, _ := do(t, "POST", s2.URL, map[string]string{"Origin": "https://wallet.example"}, `{"method":"getSlot"}`); resp.StatusCode != 200 {
		t.Fatalf("allowed origin refused: %d", resp.StatusCode)
	}
}

// Mixnet client down: 502 with a JSON-RPC error, for HTTP and websockets,
// and no direct connection is ever attempted.
func TestNoClearnetFallback(t *testing.T) {
	w := newWorld(t)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()
	cl := &sol.Client{D: &mixnet.Dialer{Proxy: dead, Timeout: time.Second}, Timeout: 3 * time.Second}
	srv := httptest.NewServer(&gateway.Gateway{Client: cl, Nets: w.registry(t), Default: "mainnet"})
	defer srv.Close()
	resp, body := do(t, "POST", srv.URL, nil, `{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)
	var e struct {
		Error struct {
			Code    int
			Message string
			Data    struct{ Reason string }
		}
	}
	if resp.StatusCode != 502 || json.Unmarshal([]byte(body), &e) != nil || e.Error.Data.Reason != "MixnetUnavailable" || !strings.Contains(e.Error.Message, "no SOCKS5 proxy") {
		t.Fatalf("want 502 MixnetUnavailable, got %d %s", resp.StatusCode, body)
	}
	if resp, _ = do(t, "POST", srv.URL+"/devnet", nil, `{"method":"sendTransaction","params":["`+testTx+`"]}`); resp.StatusCode != 502 {
		t.Fatalf("send: want 502, got %d", resp.StatusCode)
	}
	if _, _, status := wsDial(t, srv.URL, "/devnet", nil); status != 502 {
		t.Fatalf("websocket: want 502, got %d", status)
	}
	if len(w.got) != 0 {
		t.Fatalf("an upstream was reached without the mixnet: %v", w.got)
	}
	// The mixnet client dies after two upstreams passed their genesis check:
	// the request stops at the first one instead of walking the list.
	d2 := &mixnet.Dialer{Proxy: w.socks.Addr, DNS: mixnet.DNSDoH, RootCAs: w.d.RootCAs, Resolvers: []string{"1.1.1.1"}, Timeout: time.Second}
	cl2 := &sol.Client{D: d2, Timeout: 3 * time.Second, Log: w.logf}
	m := &sol.Network{Name: "mainnet", Genesis: mainGenesis}
	var slot uint64
	for _, u := range []string{"api.mainnet-beta.solana.com", "other.example"} {
		m.Upstreams = ups(u)
		if _, err := cl2.Call(context.Background(), m, "getSlot", nil, &slot); err != nil {
			t.Fatal(err)
		}
	}
	d2.Proxy = dead
	cl2.CloseIdle()
	m.Upstreams = ups("api.mainnet-beta.solana.com", "other.example")
	n := len(w.logs)
	if _, err := cl2.Call(context.Background(), m, "getSlot", nil, &slot); err == nil {
		t.Fatal("answered with the proxy down")
	}
	if _, err := cl2.DialWS(context.Background(), m); err == nil {
		t.Fatal("websocket with the proxy down")
	}
	if tries := strings.Count(strings.Join(w.logs[n:], "\n"), "no SOCKS5 proxy"); tries != 2 {
		t.Fatalf("%d attempts with the proxy down (want 1 per call):\n%s", tries, strings.Join(w.logs[n:], "\n"))
	}
}

// The upstream is a real, directly reachable TLS server on localhost (RPC and
// websocket). With the mixnet client down, nothing may reach it, in either
// DNS mode and at any layer: a direct dial would land here and be seen.
func TestNoFallbackEvenWhenDirectWouldWork(t *testing.T) {
	ca := fakes.NewCA()
	var mu sync.Mutex
	hits := 0
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{ca.Leaf("localhost", "127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		fmt.Fprintf(rw, `{"jsonrpc":"2.0","result":"%s","id":1}`, mainGenesis)
	})}
	go srv.Serve(ln)
	defer srv.Close()
	dl, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := dl.Addr().String()
	dl.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	reg := sol.NewRegistry()
	if err := reg.Add("mainnet", []string{"https://localhost:" + port, "https://127.0.0.1:" + port}); err != nil {
		t.Fatal(err)
	}
	m, _ := reg.Get("mainnet")
	for _, mode := range []string{mixnet.DNSDoH, mixnet.DNSExit} {
		cl := &sol.Client{D: &mixnet.Dialer{Proxy: dead, DNS: mode, RootCAs: ca.Pool, Timeout: time.Second}, Timeout: 3 * time.Second}
		gw := httptest.NewServer(&gateway.Gateway{Client: cl, Nets: reg, Default: "mainnet"})
		resp, body := do(t, "POST", gw.URL, nil, `{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)
		if resp.StatusCode != 502 || !strings.Contains(body, "no SOCKS5 proxy") {
			t.Errorf("dns=%s: want 502, got %d %s", mode, resp.StatusCode, body)
		}
		if _, _, status := wsDial(t, gw.URL, "/", nil); status != 502 {
			t.Errorf("dns=%s: websocket: want 502, got %d", mode, status)
		}
		gw.Close()
		var slot uint64
		if _, err := cl.Call(context.Background(), m, "getSlot", nil, &slot); err == nil {
			t.Errorf("dns=%s: CLI path answered without the mixnet", mode)
		}
		if _, err := cl.DialWS(context.Background(), m); err == nil {
			t.Errorf("dns=%s: CLI websocket without the mixnet", mode)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Fatalf("the upstream was reached directly %d times", hits)
	}
}

func TestFailoverAndRedirects(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	reg := w.registry(t)
	m, _ := reg.Get("mainnet")
	var slot uint64
	// 429, 500, 403 "blocked", then a node that passes genesis and answers 503, then a good one.
	m.Upstreams = ups("busy.example", "bad.example", "blocked.example", "late.example", "api.mainnet-beta.solana.com")
	used, err := w.cl.Call(ctx, m, "getSlot", nil, &slot)
	if err != nil || used != "https://api.mainnet-beta.solana.com" {
		t.Fatalf("failover: %v %s", err, used)
	}
	if c := w.calls("late.example"); len(c) != 2 || c[1] != "getSlot" {
		t.Fatalf("late.example saw %v", c)
	}
	if !strings.Contains(w.allLogs(), "HTTP 403 (Your IP or provider is blocked from this endpoint)") {
		t.Fatalf("blocked reason not logged:\n%s", w.allLogs())
	}
	// A node that passed the genesis check but refuses one call (403, as
	// publicnode does for indexed calls without a token): the next one answers.
	m.Upstreams = ups("indexed.example", "other.example")
	if used, err = w.cl.Call(ctx, m, "getTokenAccountsByOwner", []any{testPubkey}, nil); err != nil || used != "https://other.example" {
		t.Fatalf("403 failover: %v %s", err, used)
	}
	if c := w.calls("indexed.example"); len(c) != 2 || c[1] != "getTokenAccountsByOwner" {
		t.Fatalf("indexed.example saw %v", c)
	}
	// Every upstream failing with HTTP errors: the last answer is surfaced.
	m.Upstreams = ups("busy.example", "late.example")
	status, _, _, err := w.cl.Fetch(ctx, m, sol.Request{Body: []byte(`{"method":"getSlot"}`)})
	if err != nil || status != 503 {
		t.Fatalf("all-5xx: status %d err %v", status, err)
	}
	// A timeout moves on to the next upstream.
	fast := &sol.Client{D: w.d, Timeout: 700 * time.Millisecond, Log: w.logf}
	m.Upstreams = ups("slow.example", "api.mainnet-beta.solana.com")
	t0 := time.Now()
	used, err = fast.Call(ctx, m, "getSlot", nil, &slot)
	if err != nil || used != "https://api.mainnet-beta.solana.com" || time.Since(t0) > 4*time.Second {
		t.Fatalf("timeout failover: %v %s after %s", err, used, time.Since(t0))
	}
	if !strings.Contains(w.allLogs(), "no answer within") {
		t.Fatalf("timeout not logged:\n%s", w.allLogs())
	}
	// A POST is re-sent with its body to the next upstream.
	m.Upstreams = ups("late.example", "other.example")
	send := []byte(`{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["` + testTx + `"]}`)
	status, body, used, err := w.cl.Fetch(ctx, m, sol.Request{Method: "POST", Body: send})
	if err != nil || status != 200 || used != "https://other.example" || w.last("other.example").Body != string(send) || !strings.Contains(string(body), "prior credit") {
		t.Fatalf("POST failover: %d %s %v", status, body, err)
	}
	// A redirect is never followed (its target was never genesis checked),
	// neither at the genesis check nor later, and its Location never comes back.
	m.Upstreams = ups("redir.example", "other.example")
	before := len(w.calls("other.example"))
	if used, err = w.cl.Call(ctx, m, "getSlot", nil, &slot); err != nil || used != "https://other.example" {
		t.Fatalf("redirect: %v %s", err, used)
	}
	if c := w.calls("other.example")[before:]; len(c) != 1 {
		t.Fatalf("redirect was followed to other.example: %v", c)
	}
	// Every upstream redirecting a call: an error, never the redirect.
	m.Upstreams = ups("api.mainnet-beta.solana.com", "other.example")
	if status, _, _, err := w.cl.Fetch(ctx, m, sol.Request{Body: []byte(`{"method":"redirect"}`)}); err == nil {
		t.Fatalf("redirect answer passed through (HTTP %d)", status)
	}
	if c := w.calls("other.example"); c[len(c)-1] != "redirect" {
		t.Fatalf("a redirect did not move on to the next upstream: %v", c)
	}
	srv := w.gateway(t, "mainnet")
	if resp, _ := do(t, "POST", srv.URL, nil, `{"method":"redirect"}`); resp.StatusCode != 502 || resp.Header.Get("Location") != "" {
		t.Fatalf("redirect through the gateway: %d %v", resp.StatusCode, resp.Header)
	}
	assertOnlyMixnet(t, w, "192.0.2.6", "192.0.2.7", "192.0.2.8", "192.0.2.9", "192.0.2.10", "192.0.2.13", "192.0.2.14", "192.0.2.16")
}

func TestOnlyHTTPSUpstreams(t *testing.T) {
	w := newWorld(t)
	// A plain-http RPC URL: refused before any stream opens.
	n := &sol.Network{Name: "x", Upstreams: []sol.Upstream{{RPC: "http://api.mainnet-beta.solana.com"}}}
	if _, err := w.cl.Call(context.Background(), n, "getSlot", nil, nil); err == nil {
		t.Fatal("plain http upstream accepted")
	}
	if _, err := w.cl.DialWS(context.Background(), n); err == nil {
		t.Fatal("plain http upstream accepted for a websocket")
	}
	if s := w.socks.Seen(); len(s) != 0 {
		t.Fatalf("plain http upstream opened streams: %v", s)
	}
	// A ws:// PubSub URL: never gets a websocket.
	n.Upstreams = []sol.Upstream{{RPC: "https://api.mainnet-beta.solana.com", WS: "ws://api.mainnet-beta.solana.com"}}
	if _, err := w.cl.DialWS(context.Background(), n); err == nil {
		t.Fatal("ws:// upstream accepted")
	}
	for _, r := range w.socks.Seen() {
		if r.Port != 443 {
			t.Fatalf("stream to port %d", r.Port)
		}
	}
	for _, s := range w.got {
		if s.Method == "UPGRADE" {
			t.Fatal("a ws:// upstream got a websocket")
		}
	}
}

// Request params (pubkeys, signatures, transactions) never reach a log line
// or an error answer, on success or failure, HTTP or websocket.
func TestLogsCarryNoParams(t *testing.T) {
	w := newWorld(t)
	srv := w.gateway(t, "mainnet")
	reqs := func(url string) []string {
		_, b1 := do(t, "POST", url, nil, `{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["`+testPubkey+`"]}`)
		_, b2 := do(t, "POST", url, nil, `{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["`+testTx+`",{"encoding":"base64"}]}`)
		_, b3 := do(t, "POST", url, nil, `[{"method":"getSignatureStatuses","params":[["`+testSig+`"]]},{"method":"getAccountInfo","params":["`+testPubkey+`"]}]`)
		_, b4 := do(t, "POST", url, nil, `{"method":"`+testPubkey+`"}`)
		return []string{b1, b2, b3, b4}
	}
	reqs(srv.URL)
	conn, br, _ := wsDial(t, srv.URL, "/", nil)
	wsRoundTrip(t, conn, br, `{"jsonrpc":"2.0","id":1,"method":"signatureSubscribe","params":["`+testSig+`"]}`)
	conn.Close()
	time.Sleep(100 * time.Millisecond)
	all := w.allLogs()
	for _, want := range []string{"mainnet getBalance", "mainnet sendTransaction", "batch of 2 (getSignatureStatuses x1, getAccountInfo x1)", "mainnet * ", "websocket signatureSubscribe"} {
		if !strings.Contains(all, want) {
			t.Errorf("log lacks %q:\n%s", want, all)
		}
	}
	// The same with the mixnet client down: errors and logs stay clean too.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()
	cl := &sol.Client{D: &mixnet.Dialer{Proxy: dead, Timeout: time.Second}, Timeout: 3 * time.Second, Log: w.logf}
	down := httptest.NewServer(&gateway.Gateway{Client: cl, Nets: w.registry(t), Default: "mainnet", Log: w.logf})
	defer down.Close()
	failBodies := reqs(down.URL)
	// An upstream that drops the connection after its genesis check.
	reg := w.registry(t)
	m, _ := reg.Get("mainnet")
	m.Upstreams = ups("drop.example")
	dropGW := httptest.NewServer(&gateway.Gateway{Client: w.cl, Nets: reg, Default: "mainnet", Log: w.logf})
	defer dropGW.Close()
	failBodies = append(failBodies, reqs(dropGW.URL)...)
	if c := w.calls("drop.example"); len(c) < 2 || c[1] != "getBalance" {
		t.Fatalf("drop.example saw %v", c)
	}
	// A custom upstream with an API key in its path (as QuickNode-style URLs
	// have) that drops the connection: Go's *url.Error would carry the URL.
	m.Upstreams = []sol.Upstream{{RPC: "https://drop.example/secret-token-123"}}
	_, b := do(t, "POST", dropGW.URL, nil, `{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)
	failBodies = append(failBodies, b)
	all = w.allLogs()
	for _, secret := range []string{testPubkey, testTx, testSig, testTx[:20], "secret-token-123"} {
		if strings.Contains(all, secret) {
			t.Errorf("log contains %q:\n%s", secret, all)
		}
		for _, b := range failBodies {
			if strings.Contains(b, secret) {
				t.Errorf("error answer contains %q: %s", secret, b)
			}
		}
	}
}
