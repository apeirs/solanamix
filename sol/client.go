package sol

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/apeirs/solanamix/mixnet"
	"github.com/apeirs/solanamix/wsx"
)

// UserAgent is sent upstream instead of whatever the local caller used.
const UserAgent = "Mozilla/5.0"

// Client sends requests to upstream Solana RPC endpoints, always via the
// mixnet Dialer. There is no clearnet fallback.
type Client struct {
	D       *mixnet.Dialer
	Timeout time.Duration // per upstream attempt (genesis check included), until the response headers arrive
	Log     func(format string, args ...any)

	once     sync.Once
	http     *http.Client
	mu       sync.Mutex
	verified map[string]bool // upstream|network pairs whose genesis was checked
	Calls    atomic.Int64
}

func (c *Client) logf(f string, a ...any) {
	if c.Log != nil {
		c.Log(f, a...)
	}
}

func (c *Client) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 50 * time.Second
}

func (c *Client) client() *http.Client {
	c.once.Do(func() {
		// Keep-alives on: one mixnet stream + TLS handshake is expensive, reuse it.
		c.http = &http.Client{
			Transport: &http.Transport{
				Proxy:               nil, // never an environment proxy
				DialContext:         c.D.DialContext,
				TLSHandshakeTimeout: 90 * time.Second,
				TLSClientConfig:     &tls.Config{RootCAs: c.D.RootCAs},
				IdleConnTimeout:     90 * time.Second,
				MaxIdleConnsPerHost: 4,
			},
			// A redirect is never followed: its target was not genesis
			// checked. It counts as a failed upstream, and its Location never
			// reaches the caller (who would follow it over the clearnet).
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	})
	return c.http
}

// CloseIdle drops the kept-alive upstream streams, so the next request opens
// a fresh one (after the mixnet client restarted, the old ones are dead).
func (c *Client) CloseIdle() {
	c.client().CloseIdleConnections()
}

// Request is one HTTP request relative to an upstream RPC URL.
type Request struct {
	Method      string // POST (JSON-RPC) or GET (/health)
	Path        string // "" (the RPC URL itself) or "/health"
	Body        []byte
	ContentType string
	Label       string // for logs only; must never contain request params
}

// Response is an upstream answer. The caller closes Body.
type Response struct {
	Status      int
	ContentType string
	Body        io.ReadCloser
	Upstream    string
}

// Join appends a path to an upstream RPC URL.
func Join(base, path string) (*url.URL, error) {
	if err := CheckURL(base, "https"); err != nil {
		return nil, err
	}
	u, err := url.Parse(strings.TrimRight(base, "/") + path)
	if err != nil {
		return nil, err
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u, nil
}

// HostOf returns the host of an upstream URL, for logs.
func HostOf(u string) string {
	if p, err := url.Parse(u); err == nil && p.Host != "" {
		return p.Host
	}
	return u
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b cancelBody) Close() error { err := b.ReadCloser.Close(); b.cancel(); return err }

// retryable: provider-side trouble worth trying the next upstream for. A
// redirect counts (it is never followed), and so does 401/403: a public
// endpoint that refuses the exit gateway's IP.
func retryable(status int) bool {
	return status >= 500 || status == 429 || status == 401 || status == 403 || (status >= 300 && status < 400)
}

// send is one HTTP exchange with only our own headers.
func (c *Client) send(ctx context.Context, method string, u *url.URL, body []byte, ct string) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	hr, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return nil, err
	}
	// Nothing about the local caller is forwarded: no Origin, Referer,
	// cookies, wallet User-Agent or solana-client header.
	hr.Header = http.Header{}
	hr.Header.Set("User-Agent", UserAgent)
	hr.Header.Set("Accept", "application/json, */*;q=0.5")
	if body != nil {
		if ct == "" {
			ct = "application/json"
		}
		hr.Header.Set("Content-Type", ct)
	}
	resp, err := c.client().Do(hr)
	if err != nil {
		// *url.Error carries the full URL; keep only the cause.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, err
	}
	return resp, nil
}

var genesisReq = []byte(`{"jsonrpc":"2.0","id":1,"method":"getGenesisHash"}`)

// verify checks once per upstream and network that the upstream's genesis
// hash is the network's. An upstream of another cluster (a devnet URL under
// mainnet, a typo in --add) never sees a real request or websocket.
func (c *Client) verify(ctx context.Context, n *Network, base string) error {
	key := base + "|" + n.Name
	c.mu.Lock()
	done := c.verified[key]
	c.mu.Unlock()
	if done {
		return nil
	}
	u, err := Join(base, "")
	if err != nil {
		return err
	}
	resp, err := c.send(ctx, "POST", u, genesisReq, "application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return &statusError{resp.StatusCode, "genesis check", errorMessage(resp.Body)}
	}
	var b struct {
		Result string    `json:"result"`
		Error  *RPCError `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&b); err != nil {
		return fmt.Errorf("genesis check: not a Solana RPC answer")
	}
	if b.Error != nil {
		return fmt.Errorf("genesis check: %s", oneLine(b.Error.Message, 120))
	}
	if h, err := DecodeBase58(b.Result); err != nil || len(h) != 32 {
		return errors.New("genesis check: no genesis hash")
	}
	if err := n.CheckGenesis(b.Result); err != nil {
		return err
	}
	c.mu.Lock()
	if c.verified == nil {
		c.verified = map[string]bool{}
	}
	c.verified[key] = true
	c.mu.Unlock()
	return nil
}

type statusError struct {
	status int
	what   string
	msg    string
}

func (e *statusError) Error() string {
	if e.msg != "" {
		return fmt.Sprintf("%s: HTTP %d (%s)", e.what, e.status, e.msg)
	}
	return fmt.Sprintf("%s: HTTP %d", e.what, e.status)
}

// errorMessage is the JSON-RPC error message of an error answer, if any
// (e.g. "Your IP or provider is blocked from this endpoint").
func errorMessage(r io.Reader) string {
	var b struct {
		Error *RPCError `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(r, 4096)).Decode(&b) != nil || b.Error == nil {
		return ""
	}
	return oneLine(b.Error.Message, 100)
}

// Verified reports whether base has passed the genesis check for n.
func (c *Client) Verified(n *Network, base string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.verified[base+"|"+n.Name]
}

// Do sends req to the first upstream of n that passes the genesis check and
// answers without a provider error. Other answers (a JSON-RPC error is an
// answer) are returned as they are. If every upstream answered
// 5xx/429/401/403/3xx, the last such answer is returned (never a redirect).
func (c *Client) Do(ctx context.Context, n *Network, req Request) (*Response, error) {
	if len(n.Upstreams) == 0 {
		return nil, fmt.Errorf("network %s has no upstream", n.Name)
	}
	if req.Method == "" {
		req.Method = "POST"
	}
	var errs []error
	var fallback *Response
	for _, up := range n.Upstreams {
		base := up.RPC
		u, err := Join(base, req.Path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		host := u.Host
		tmo := c.timeout()
		actx, cancel := context.WithCancel(ctx)
		timer := time.AfterFunc(tmo, cancel)
		t0 := time.Now()
		fail := func(err error) {
			cancel()
			c.logf("%-4s %s %s failed after %.2fs: %v", "up", host, req.Label, time.Since(t0).Seconds(), err)
			errs = append(errs, fmt.Errorf("%s: %w", host, err))
		}
		if err := c.verify(actx, n, base); err != nil {
			if !timer.Stop() {
				err = fmt.Errorf("no answer within %s", tmo)
			}
			fail(err)
			if ctx.Err() != nil || errors.Is(err, mixnet.ErrProxyUnreachable) {
				break // the next upstream would need the same missing proxy
			}
			continue
		}
		resp, err := c.send(actx, req.Method, u, req.Body, req.ContentType)
		stopped := timer.Stop()
		if err != nil {
			if !stopped {
				err = fmt.Errorf("no answer within %s", tmo)
			}
			fail(err)
			if ctx.Err() != nil || errors.Is(err, mixnet.ErrProxyUnreachable) {
				break
			}
			continue
		}
		if retryable(resp.StatusCode) {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			cancel()
			c.logf("%-4s %s %s answered HTTP %d in %.2fs, trying the next upstream", "up", host, req.Label, resp.StatusCode, time.Since(t0).Seconds())
			errs = append(errs, fmt.Errorf("%s: HTTP %d", host, resp.StatusCode))
			if resp.StatusCode < 300 || resp.StatusCode >= 400 {
				fallback = &Response{Status: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"), Body: io.NopCloser(bytes.NewReader(b)), Upstream: base}
			}
			continue
		}
		c.Calls.Add(1)
		c.logf("%-4s %s %s answered HTTP %d in %.2fs", "up", host, req.Label, resp.StatusCode, time.Since(t0).Seconds())
		return &Response{Status: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"),
			Body: cancelBody{resp.Body, cancel}, Upstream: base}, nil
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, errors.Join(errs...)
}

// Fetch is Do with the answer read whole (at most 64 MiB).
func (c *Client) Fetch(ctx context.Context, n *Network, req Request) (int, []byte, string, error) {
	if req.Label == "" {
		req.Label = Label(req.Body)
	}
	resp, err := c.Do(ctx, n, req)
	if err != nil {
		return 0, nil, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	return resp.Status, b, resp.Upstream, err
}

// RPCError is a JSON-RPC error object, or an HTTP error answer.
type RPCError struct {
	Code     int             `json:"code"`
	Message  string          `json:"message"`
	Data     json.RawMessage `json:"data,omitempty"`
	Status   int             `json:"-"`
	Upstream string          `json:"-"`
}

func (e *RPCError) Error() string {
	s := fmt.Sprintf("%s (code %d)", oneLine(e.Message, 300), e.Code)
	if e.Status != 0 && e.Status != 200 {
		s = fmt.Sprintf("HTTP %d: %s", e.Status, oneLine(e.Message, 300))
	}
	if e.Upstream != "" {
		s += " from " + HostOf(e.Upstream)
	}
	return s
}

// Logs returns the program logs a preflight failure carries, if any.
func (e *RPCError) Logs() []string {
	var d struct {
		Logs []string `json:"logs"`
	}
	_ = json.Unmarshal(e.Data, &d)
	return d.Logs
}

func oneLine(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > max {
		s = s[:max] + "..."
	}
	if !utf8.ValidString(s) {
		return fmt.Sprintf("%.*q", max, s)
	}
	return s
}

// Call runs one JSON-RPC method and decodes its result into v.
func (c *Client) Call(ctx context.Context, n *Network, method string, params any, v any) (string, error) {
	req := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method}
	if params != nil {
		req["params"] = params
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	status, b, used, err := c.Fetch(ctx, n, Request{Method: "POST", Body: body, ContentType: "application/json", Label: Label(body)})
	if err != nil {
		return used, err
	}
	var r struct {
		Result json.RawMessage `json:"result"`
		Error  *RPCError       `json:"error"`
	}
	if json.Unmarshal(b, &r) != nil || (r.Error == nil && r.Result == nil) {
		if status != 200 {
			return used, &RPCError{Status: status, Message: oneLine(string(b), 200), Upstream: used}
		}
		return used, fmt.Errorf("unexpected answer from %s: %.120q", HostOf(used), b)
	}
	if r.Error != nil {
		r.Error.Status, r.Error.Upstream = status, used
		return used, r.Error
	}
	if v == nil {
		return used, nil
	}
	if err := json.Unmarshal(r.Result, v); err != nil {
		return used, fmt.Errorf("unexpected %s result from %s: %v", method, HostOf(used), err)
	}
	return used, nil
}

// WSConn is an open PubSub websocket to an upstream, through the mixnet.
// Reader holds bytes the upstream sent after its 101.
type WSConn struct {
	net.Conn
	Reader   *bufio.Reader
	Upstream string // the wss URL
}

// DialWS opens the PubSub websocket of the first upstream of n whose RPC URL
// passes the genesis check and whose websocket handshake succeeds. The
// handshake sends only Host, the websocket headers and a fixed User-Agent.
func (c *Client) DialWS(ctx context.Context, n *Network) (*WSConn, error) {
	if len(n.Upstreams) == 0 {
		return nil, fmt.Errorf("network %s has no upstream", n.Name)
	}
	var errs []error
	for _, up := range n.Upstreams {
		ws := up.WSURL()
		u, err := url.Parse(ws)
		if err != nil || CheckURL(ws, "wss") != nil {
			errs = append(errs, fmt.Errorf("%s: not a wss URL", HostOf(ws)))
			continue
		}
		host := u.Host
		addr := host
		if u.Port() == "" {
			addr = net.JoinHostPort(u.Hostname(), "443")
		}
		path := u.EscapedPath()
		if path == "" {
			path = "/"
		}
		t0 := time.Now()
		actx, cancel := context.WithTimeout(ctx, c.timeout())
		conn, br, err := func() (net.Conn, *bufio.Reader, error) {
			if err := c.verify(actx, n, up.RPC); err != nil {
				return nil, nil, err
			}
			tc, err := c.D.DialTLS(actx, addr, &tls.Config{ServerName: u.Hostname(), RootCAs: c.D.RootCAs, NextProtos: []string{"http/1.1"}})
			if err != nil {
				return nil, nil, err
			}
			if dl, ok := actx.Deadline(); ok {
				_ = tc.SetDeadline(dl)
			}
			stop := context.AfterFunc(actx, func() { _ = tc.SetDeadline(time.Unix(1, 0)) })
			br, err := wsx.Handshake(tc, host, path, UserAgent)
			stop()
			if err != nil {
				tc.Close()
				return nil, nil, err
			}
			_ = tc.SetDeadline(time.Time{})
			return tc, br, nil
		}()
		timedOut := actx.Err() != nil && ctx.Err() == nil
		cancel()
		if err != nil {
			if timedOut {
				err = fmt.Errorf("no websocket within %s", c.timeout())
			}
			c.logf("%-4s %s websocket failed after %.2fs: %v", "up", host, time.Since(t0).Seconds(), err)
			errs = append(errs, fmt.Errorf("%s: %w", host, err))
			if ctx.Err() != nil || errors.Is(err, mixnet.ErrProxyUnreachable) {
				break
			}
			continue
		}
		c.logf("%-4s %s websocket open in %.2fs", "up", host, time.Since(t0).Seconds())
		return &WSConn{Conn: conn, Reader: br, Upstream: ws}, nil
	}
	return nil, errors.Join(errs...)
}
