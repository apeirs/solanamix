// Package gateway is a local Solana RPC endpoint for wallets and tools.
// Point the Solana CLI, @solana/web3.js, a wallet's custom RPC setting or
// curl at
//
//	http://127.0.0.1:8899            the default network (mainnet)
//	http://127.0.0.1:8899/devnet     devnet (also /testnet, /mainnet, and any --add network)
//	ws://127.0.0.1:8900[/devnet]     PubSub (subscriptions), as clients derive it: port + 1
//
// and every JSON-RPC request and PubSub websocket goes to a public Solana
// endpoint through the Nym mixnet, after the endpoint passed a genesis check.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apeirs/solanamix/sol"
	"github.com/apeirs/solanamix/wsx"
)

// Gateway serves the routes above.
type Gateway struct {
	Client  *sol.Client
	Nets    *sol.Registry
	Default string   // network served at the root ("" = none)
	Hosts   []string // extra accepted Host values
	Origins []string // extra accepted Origin values ("*" = any web page)
	Timeout time.Duration
	Log     func(string, ...any)

	Total, Failed, Sockets atomic.Int64
}

func (g *Gateway) logf(f string, a ...any) {
	if g.Log != nil {
		g.Log(f, a...)
	}
}

func (g *Gateway) hostOK(h string) bool {
	if hh, _, err := net.SplitHostPort(h); err == nil {
		h = hh
	}
	h = strings.Trim(h, "[]")
	switch h {
	case "127.0.0.1", "localhost", "::1":
		return true
	}
	for _, x := range g.Hosts {
		if x == h {
			return true
		}
	}
	return false
}

// originOK: no Origin (CLI tools, node scripts), a browser extension (a
// wallet's custom RPC setting), or an origin the user allowed. Any other web
// page is refused, so a site you visit can't use your gateway.
func (g *Gateway) originOK(o string) bool {
	if o == "" {
		return true
	}
	for _, p := range []string{"chrome-extension://", "moz-extension://", "safari-web-extension://"} {
		if strings.HasPrefix(o, p) {
			return true
		}
	}
	for _, x := range g.Origins {
		if x == "*" || x == o {
			return true
		}
	}
	return false
}

// route maps a request path to a network and the upstream path: "" (the RPC
// URL itself) or "/health". Nothing else on an upstream host is reachable.
func (g *Gateway) route(escaped string) (*sol.Network, string, bool) {
	if strings.Contains(escaped, "%") || strings.Contains(escaped, "//") {
		return nil, "", false
	}
	segs := strings.Split(strings.Trim(escaped, "/"), "/")
	if len(segs) == 1 && segs[0] == "" {
		segs = nil
	}
	var name, sub string
	switch len(segs) {
	case 0:
		name = g.Default
	case 1:
		if segs[0] == "health" {
			name, sub = g.Default, "/health"
		} else {
			name = segs[0]
		}
	case 2:
		if segs[1] != "health" {
			return nil, "", false
		}
		name, sub = segs[0], "/health"
	default:
		return nil, "", false
	}
	if name == "" {
		return nil, "", false
	}
	n, ok := g.Nets.Get(name)
	if !ok {
		return nil, "", false
	}
	return n, sub, true
}

// bodyType maps the caller's Content-Type to the one sent upstream. JSON-RPC
// clients post JSON; `curl -d '{...}'`, as in most RPC examples, labels it a
// form: it goes up as JSON. (A web page could post a form cross-site without
// a preflight, but its Origin header is refused before this point.)
func bodyType(ct string) (string, bool) {
	if ct == "" {
		return "application/json", true
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return "", false
	}
	switch mt {
	case "application/json", "text/plain", "application/x-www-form-urlencoded":
		return "application/json", true
	}
	return "", false
}

// headerList keeps a CORS request-header list to plain header tokens.
func headerList(s string) string {
	var out []string
	for _, h := range strings.Split(s, ",") {
		h = strings.TrimSpace(h)
		if h != "" && len(h) < 64 && strings.Trim(strings.ToLower(h), "abcdefghijklmnopqrstuvwxyz0123456789-") == "" {
			out = append(out, h)
		}
	}
	if len(out) == 0 {
		return "content-type"
	}
	return strings.Join(out, ", ")
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// DNS-rebinding guard: a web page must not reach this through a rebound name.
	if !g.hostOK(r.Host) {
		http.Error(w, "bad Host header", http.StatusForbidden)
		return
	}
	origin := r.Header.Get("Origin")
	if !g.originOK(origin) {
		http.Error(w, "origin not allowed (see --allow-origin)", http.StatusForbidden)
		return
	}
	h := w.Header()
	if origin != "" {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Vary", "Origin")
	}
	if r.Method == "OPTIONS" {
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		// Whatever the caller asks for is allowed: none of it is forwarded.
		h.Set("Access-Control-Allow-Headers", headerList(r.Header.Get("Access-Control-Request-Headers")))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	n, sub, ok := g.route(r.URL.EscapedPath())
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		g.help(w)
		return
	}
	if wsx.IsUpgrade(r) {
		if sub != "" {
			http.Error(w, "websocket only on the RPC path", http.StatusNotFound)
			return
		}
		g.pubsub(w, r, n)
		return
	}
	if len(n.Upstreams) == 0 {
		http.Error(w, fmt.Sprintf("network %s has no upstream (add one with --add)", n.Name), http.StatusNotFound)
		return
	}
	switch {
	case r.Method == "GET" && sub == "":
		g.help(w)
		return
	case r.Method == "GET" && sub == "/health":
	case r.Method == "POST" && sub == "":
	default:
		http.Error(w, "POST JSON-RPC to the RPC path, GET /health, or a websocket", http.StatusMethodNotAllowed)
		return
	}
	var body []byte
	var ct string
	if r.Method == "POST" {
		var ok bool
		if ct, ok = bodyType(r.Header.Get("Content-Type")); !ok {
			http.Error(w, "POST body must be JSON (application/json, text/plain or curl's default form type)", http.StatusUnsupportedMediaType)
			return
		}
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, 8<<20))
		if err != nil {
			http.Error(w, "could not read the request body", http.StatusBadRequest)
			return
		}
		if body == nil {
			body = []byte{}
		}
	}
	label := "GET /health"
	if r.Method == "POST" {
		label = sol.Label(body)
	}
	g.Total.Add(1)
	tmo := g.Timeout
	if tmo == 0 {
		tmo = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(r.Context(), tmo)
	defer cancel()
	t0 := time.Now()
	resp, err := g.Client.Do(ctx, n, sol.Request{Method: r.Method, Path: sub, Body: body, ContentType: ct, Label: label})
	if err != nil {
		g.Failed.Add(1)
		g.logf("%s %s FAILED after %.1fs: %v", n.Name, label, time.Since(t0).Seconds(), err)
		g.fail(w, err)
		return
	}
	defer resp.Body.Close()
	// Only the content type comes back: no Set-Cookie, no Location, no server
	// headers that would tempt the caller to talk to the upstream directly.
	if resp.ContentType != "" {
		h.Set("Content-Type", resp.ContentType)
	}
	w.WriteHeader(resp.Status)
	nb, _ := io.Copy(w, resp.Body)
	g.logf("%s %s -> mixnet -> %s HTTP %d, %d bytes in %.1fs", n.Name, label, sol.HostOf(resp.Upstream), resp.Status, nb, time.Since(t0).Seconds())
}

// fail answers 502 with a JSON-RPC error object, which web3.js and the
// Solana CLI show to the caller.
func (g *Gateway) fail(w http.ResponseWriter, err error) {
	msg := fmt.Sprintf("solanamix: all upstreams failed over the mixnet (no clearnet fallback): %v", err)
	js, _ := json.Marshal(msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	fmt.Fprintf(w, `{"jsonrpc":"2.0","error":{"code":-32603,"message":%s,"data":{"reason":"MixnetUnavailable"}},"id":null}`, js)
}

// pubsub relays a PubSub websocket to an upstream's, through the mixnet.
// The upstream socket is opened (genesis check first) before the local
// handshake is answered, so a client never sees a socket that goes nowhere.
func (g *Gateway) pubsub(w http.ResponseWriter, r *http.Request, n *sol.Network) {
	key, err := wsx.CheckClient(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket not supported here", http.StatusInternalServerError)
		return
	}
	g.Sockets.Add(1)
	t0 := time.Now()
	tmo := g.Timeout
	if tmo == 0 {
		tmo = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(r.Context(), tmo)
	up, err := g.Client.DialWS(ctx, n)
	cancel()
	if err != nil {
		if r.Context().Err() != nil {
			g.logf("%s websocket: the client left after %.1fs, before the upstream socket opened", n.Name, time.Since(t0).Seconds())
			return
		}
		g.Failed.Add(1)
		g.logf("%s websocket FAILED after %.1fs: %v", n.Name, time.Since(t0).Seconds(), err)
		g.fail(w, err)
		return
	}
	upHost := sol.HostOf(up.Upstream)
	conn, brw, err := hj.Hijack()
	if err != nil {
		up.Close()
		return
	}
	_ = conn.SetDeadline(time.Time{})
	if _, err := io.WriteString(conn, wsx.AcceptResponse(key)); err != nil {
		conn.Close()
		up.Close()
		return
	}
	g.logf("%s websocket open -> mixnet -> %s in %.1fs", n.Name, upHost, time.Since(t0).Seconds())
	done := make(chan struct{}, 2)
	var down int64
	msgs := 0
	go func() {
		// upstream -> client, byte for byte (server frames are not masked).
		down, _ = io.Copy(conn, up.Reader)
		done <- struct{}{}
	}()
	go func() {
		// client -> upstream, frame by frame, so the log can name the
		// methods (never their params). Frames go up exactly as received.
		for {
			f, err := wsx.ReadFrame(brw.Reader)
			if err != nil {
				break
			}
			if f.Op == wsx.OpText && f.Fin {
				msgs++
				g.logf("%s websocket %s -> mixnet -> %s", n.Name, sol.Label(f.Payload), upHost)
			}
			if _, err := up.Write(f.Raw); err != nil {
				break
			}
			if f.Op == wsx.OpClose {
				break
			}
		}
		done <- struct{}{}
	}()
	<-done
	conn.Close()
	up.Close()
	<-done
	g.logf("%s websocket closed after %.1fs: %d messages up, %d bytes down", n.Name, time.Since(t0).Seconds(), msgs, down)
}

func (g *Gateway) help(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "solanamix gateway: Solana JSON-RPC and PubSub over the Nym mixnet.\n")
	if g.Default != "" {
		fmt.Fprintf(w, "  POST /   (and websocket /)          %s (the default network)\n", g.Default)
	}
	fmt.Fprintf(w, "  POST /<network>  websocket /<network>   that network, e.g. /devnet\n")
	fmt.Fprintf(w, "  GET /health, /<network>/health\n")
	fmt.Fprintf(w, "networks:\n")
	for _, n := range g.Nets.All() {
		fmt.Fprintf(w, "  %-10s upstreams %d\n", n.Name, len(n.Upstreams))
	}
	fmt.Fprintf(w, "requests so far: %d, websockets: %d, failed: %d\n", g.Total.Load(), g.Sockets.Load(), g.Failed.Load())
}
