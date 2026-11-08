package wsx

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestAcceptKey(t *testing.T) {
	// RFC 6455 section 1.3.
	if got := AcceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatal(got)
	}
}

func TestFrames(t *testing.T) {
	for _, n := range []int{0, 5, 125, 126, 65535, 65536, 200000} {
		for _, mask := range []bool{false, true} {
			p := bytes.Repeat([]byte("x"), n)
			var buf bytes.Buffer
			if err := WriteFrame(&buf, OpText, p, mask); err != nil {
				t.Fatal(err)
			}
			wire := append([]byte(nil), buf.Bytes()...)
			f, err := ReadFrame(&buf)
			if err != nil || !f.Fin || f.Op != OpText || f.Masked != mask || !bytes.Equal(f.Payload, p) || !bytes.Equal(f.Raw, wire) {
				t.Fatalf("n=%d mask=%v: %v", n, mask, err)
			}
			if mask && n > 0 && bytes.Contains(wire, p) {
				t.Fatalf("n=%d: masked payload sent in clear", n)
			}
		}
	}
	// Reserved bits (an extension nobody negotiated) and oversize frames.
	if _, err := ReadFrame(bytes.NewReader([]byte{0xc1, 0})); err == nil {
		t.Fatal("RSV1 accepted")
	}
	if _, err := ReadFrame(bytes.NewReader([]byte{0x81, 127, 0xff, 0, 0, 0, 0, 0, 0, 0})); err == nil {
		t.Fatal("huge frame accepted")
	}
}

func TestCheckClient(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Connection", "keep-alive, Upgrade")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	if !IsUpgrade(r) {
		t.Fatal("not an upgrade")
	}
	if k, err := CheckClient(r); err != nil || k != "dGhlIHNhbXBsZSBub25jZQ==" {
		t.Fatal(k, err)
	}
	if !strings.Contains(AcceptResponse("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
		t.Fatal("accept response")
	}
	r.Header.Set("Sec-WebSocket-Key", "short")
	if _, err := CheckClient(r); err == nil {
		t.Fatal("bad key accepted")
	}
}

// The client side refuses a server that answers with the wrong accept key,
// forces an extension, or doesn't switch protocols.
func TestHandshake(t *testing.T) {
	for name, c := range map[string]struct {
		answer func(key string) string
		ok     bool
	}{
		"good":      {func(k string) string { return AcceptResponse(k) + "\x81\x02hi" }, true},
		"wrong key": {func(string) string { return AcceptResponse("dGhlIHNhbXBsZSBub25jZQ==") }, false},
		"extension": {func(k string) string {
			return strings.Replace(AcceptResponse(k), "\r\n\r\n", "\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n", 1)
		}, false},
		"no upgrade": {func(string) string { return "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n" }, false},
	} {
		client, server := net.Pipe()
		got := make(chan *http.Request, 1)
		go func() {
			r, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				server.Close()
				return
			}
			got <- r
			io.WriteString(server, c.answer(r.Header.Get("Sec-WebSocket-Key")))
			server.Close()
		}()
		br, err := Handshake(client, "api.devnet.solana.com", "/", "Mozilla/5.0")
		if (err == nil) != c.ok {
			t.Fatalf("%s: err %v", name, err)
		}
		r := <-got
		if r.Host != "api.devnet.solana.com" || r.Header.Get("Origin") != "" || r.Header.Get("Sec-WebSocket-Extensions") != "" || r.Header.Get("User-Agent") != "Mozilla/5.0" {
			t.Fatalf("%s: handshake headers %v", name, r.Header)
		}
		if c.ok {
			// Bytes the server sent right after its 101 are kept.
			if f, err := ReadFrame(br); err != nil || string(f.Payload) != "hi" {
				t.Fatalf("%s: first frame %v", name, err)
			}
		}
		client.Close()
	}
}
