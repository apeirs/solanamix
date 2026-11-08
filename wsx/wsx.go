// Package wsx is the small part of RFC 6455 (websockets) that solanamix needs:
// the handshake on both sides, and reading and writing frames. No extensions
// and no subprotocols are ever negotiated, so frames can be relayed between
// two connections byte for byte.
package wsx

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
)

const guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Opcodes.
const (
	OpCont  = 0
	OpText  = 1
	OpBin   = 2
	OpClose = 8
	OpPing  = 9
	OpPong  = 10
)

// MaxPayload bounds one frame (Solana account notifications can be large).
const MaxPayload = 32 << 20

// AcceptKey is Sec-WebSocket-Accept for a Sec-WebSocket-Key.
func AcceptKey(key string) string {
	h := sha1.Sum([]byte(key + guid))
	return base64.StdEncoding.EncodeToString(h[:])
}

func headerHas(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// IsUpgrade reports whether r asks for a websocket.
func IsUpgrade(r *http.Request) bool {
	return headerHas(r.Header, "Upgrade", "websocket")
}

// CheckClient validates a client's opening handshake and returns its key.
func CheckClient(r *http.Request) (string, error) {
	if r.Method != "GET" {
		return "", errors.New("websocket handshake must be GET")
	}
	if !headerHas(r.Header, "Connection", "upgrade") {
		return "", errors.New("websocket handshake without Connection: upgrade")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return "", errors.New("websocket version must be 13")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if b, err := base64.StdEncoding.DecodeString(key); err != nil || len(b) != 16 {
		return "", errors.New("bad Sec-WebSocket-Key")
	}
	return key, nil
}

// AcceptResponse is the 101 answer to a client handshake with this key. No
// extension and no subprotocol is accepted.
func AcceptResponse(key string) string {
	return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
		AcceptKey(key) + "\r\n\r\n"
}

// Handshake runs the client side of the opening handshake on conn for
// host (the Host header) and path. Only these headers are sent: Host,
// Upgrade, Connection, the key, the version and userAgent. The returned
// reader holds any bytes the server sent after its 101.
func Handshake(conn net.Conn, host, path, userAgent string) (*bufio.Reader, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(nonce[:])
	if path == "" {
		path = "/"
	}
	req := "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\nUser-Agent: " + userAgent + "\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		return nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		return nil, fmt.Errorf("websocket handshake: %w", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		resp.Body.Close()
		return nil, &StatusError{resp.StatusCode}
	}
	if !headerHas(resp.Header, "Upgrade", "websocket") || resp.Header.Get("Sec-WebSocket-Accept") != AcceptKey(key) {
		return nil, errors.New("websocket handshake: bad 101 answer")
	}
	if resp.Header.Get("Sec-WebSocket-Extensions") != "" {
		return nil, errors.New("websocket handshake: server forced an extension")
	}
	return br, nil
}

// StatusError is a non-101 answer to a websocket handshake.
type StatusError struct{ Status int }

func (e *StatusError) Error() string { return fmt.Sprintf("websocket handshake: HTTP %d", e.Status) }

// Frame is one frame. Raw holds its exact bytes on the wire (for relaying);
// Payload is unmasked.
type Frame struct {
	Fin     bool
	Op      byte
	Masked  bool
	Payload []byte
	Raw     []byte
}

// ReadFrame reads one frame.
func ReadFrame(r io.Reader) (*Frame, error) {
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	raw := append([]byte(nil), h[:]...)
	f := &Frame{Fin: h[0]&0x80 != 0, Op: h[0] & 0x0f, Masked: h[1]&0x80 != 0}
	if h[0]&0x70 != 0 {
		return nil, errors.New("websocket: reserved bits set (no extension was negotiated)")
	}
	n := uint64(h[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, err
		}
		raw = append(raw, b[:]...)
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, err
		}
		raw = append(raw, b[:]...)
		n = binary.BigEndian.Uint64(b[:])
	}
	if n > MaxPayload {
		return nil, fmt.Errorf("websocket: frame of %d bytes is over the %d limit", n, MaxPayload)
	}
	var mask [4]byte
	if f.Masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return nil, err
		}
		raw = append(raw, mask[:]...)
	}
	start := len(raw)
	raw = append(raw, make([]byte, n)...)
	if _, err := io.ReadFull(r, raw[start:]); err != nil {
		return nil, err
	}
	f.Raw = raw
	f.Payload = append([]byte(nil), raw[start:]...)
	if f.Masked {
		for i := range f.Payload {
			f.Payload[i] ^= mask[i%4]
		}
	}
	return f, nil
}

// WriteFrame writes one final frame. Clients must mask (RFC 6455 5.3).
func WriteFrame(w io.Writer, op byte, payload []byte, mask bool) error {
	h := []byte{0x80 | op}
	mb := byte(0)
	if mask {
		mb = 0x80
	}
	switch n := len(payload); {
	case n < 126:
		h = append(h, mb|byte(n))
	case n <= 0xffff:
		h = append(h, mb|126)
		h = binary.BigEndian.AppendUint16(h, uint16(n))
	default:
		h = append(h, mb|127)
		h = binary.BigEndian.AppendUint64(h, uint64(n))
	}
	p := payload
	if mask {
		var m [4]byte
		if _, err := rand.Read(m[:]); err != nil {
			return err
		}
		h = append(h, m[:]...)
		p = make([]byte, len(payload))
		for i := range payload {
			p[i] = payload[i] ^ m[i%4]
		}
	}
	_, err := w.Write(append(h, p...))
	return err
}
