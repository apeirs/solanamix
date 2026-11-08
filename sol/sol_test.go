package sol

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"
)

func TestBase58(t *testing.T) {
	for _, v := range []struct {
		in  []byte
		out string
	}{
		{[]byte("Hello World!"), "2NEpo7TZRRrLZSi2U"},
		{make([]byte, 32), "11111111111111111111111111111111"}, // the System Program
		{[]byte{0, 0, 1}, "112"},
		{nil, ""},
	} {
		if got := Base58(v.in); got != v.out {
			t.Errorf("Base58(%x) = %s, want %s", v.in, got, v.out)
		}
		if back, err := DecodeBase58(v.out); err != nil || !bytes.Equal(back, v.in) && len(v.in) > 0 {
			t.Errorf("DecodeBase58(%s) = %x %v", v.out, back, err)
		}
	}
	// The SPL Token program id decodes to 32 bytes and back.
	b, err := ParsePubkey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
	if err != nil || Base58(b) != "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" {
		t.Fatalf("token program: %x %v", b, err)
	}
	for _, bad := range []string{
		"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5D0", // '0' is not base58
		"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5",   // 31 bytes
		"2NEpo7TZRRrLZSi2U",                           // short
		"0x" + strings.Repeat("ab", 32),               // hex
	} {
		if _, err := ParsePubkey(bad); err == nil {
			t.Errorf("%q accepted as a pubkey", bad)
		}
	}
}

func shortvec(n int) []byte {
	var out []byte
	for {
		c := byte(n & 0x7f)
		n >>= 7
		if n == 0 {
			return append(out, c)
		}
		out = append(out, c|0x80)
	}
}

// transfer builds a System Program transfer message (legacy, or v0 with an
// empty lookup list) from `from` to `to`.
func transfer(from, to ed25519.PublicKey, v0 bool) []byte {
	var m []byte
	if v0 {
		m = append(m, 0x80)
	}
	m = append(m, 1, 0, 1) // 1 signer, 0 read-only signed, 1 read-only unsigned
	m = append(m, shortvec(3)...)
	m = append(m, from...)
	m = append(m, to...)
	m = append(m, make([]byte, 32)...)            // System Program
	m = append(m, bytes.Repeat([]byte{7}, 32)...) // recent blockhash
	m = append(m, shortvec(1)...)
	data := binary.LittleEndian.AppendUint32(nil, 2)
	data = binary.LittleEndian.AppendUint64(data, 1)
	m = append(m, 2)
	m = append(m, shortvec(2)...)
	m = append(m, 0, 1)
	m = append(m, shortvec(len(data))...)
	m = append(m, data...)
	if v0 {
		m = append(m, shortvec(0)...)
	}
	return m
}

func signed(priv ed25519.PrivateKey, msg []byte) []byte {
	tx := append(shortvec(1), ed25519.Sign(priv, msg)...)
	return append(tx, msg...)
}

func TestParseAndCheckTx(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	to, _, _ := ed25519.GenerateKey(nil)
	for _, v0 := range []bool{false, true} {
		msg := transfer(pub, to, v0)
		raw := signed(priv, msg)
		tx, err := ParseTx(raw)
		if err != nil {
			t.Fatalf("v0=%v: %v", v0, err)
		}
		if err := tx.CheckBroadcastable(); err != nil {
			t.Fatalf("v0=%v: signed tx refused: %v", v0, err)
		}
		if (tx.Version == 0) != v0 || tx.Instructions != 1 || len(tx.Keys) != 3 || tx.Signer() != Base58(pub) || !bytes.Equal(tx.Message, msg) {
			t.Fatalf("v0=%v: parsed %+v", v0, tx)
		}
		if tx.ID() != Base58(raw[1:65]) || !bytes.Equal(tx.Bytes(), raw) {
			t.Fatalf("id %s", tx.ID())
		}
		// base64 (web3.js) and base58 inputs.
		for _, in := range []string{base64.StdEncoding.EncodeToString(raw), Base58(raw), " " + base64.StdEncoding.EncodeToString(raw) + "\n"} {
			if got, err := ParseTxInput(in); err != nil || got.ID() != tx.ID() {
				t.Fatalf("input %.20s...: %v", in, err)
			}
		}
	}
	msg := transfer(pub, to, false)
	// Unsigned (what a wallet builds before signing): all-zero signature.
	unsigned := append(append(shortvec(1), make([]byte, 64)...), msg...)
	if tx, err := ParseTx(unsigned); err != nil {
		t.Fatal(err)
	} else if err := tx.CheckBroadcastable(); err == nil || !strings.Contains(err.Error(), "not signed") {
		t.Fatalf("unsigned: %v", err)
	}
	// Changed after signing: the amount goes from 1 to 2 lamports.
	edited := signed(priv, msg)
	edited[len(edited)-8] = 2
	if tx, err := ParseTx(edited); err != nil {
		t.Fatal(err)
	} else if err := tx.CheckBroadcastable(); err == nil || !strings.Contains(err.Error(), "does not verify") {
		t.Fatalf("edited: %v", err)
	}
	// Signed by another key.
	_, other, _ := ed25519.GenerateKey(nil)
	if tx, _ := ParseTx(signed(other, msg)); tx.CheckBroadcastable() == nil {
		t.Fatal("wrong signer accepted")
	}
	// Signature count must match the header.
	two := append(append(shortvec(2), make([]byte, 128)...), msg...)
	if tx, err := ParseTx(two); err != nil || tx.CheckBroadcastable() == nil {
		t.Fatalf("2 signature slots for 1 signer: %v", err)
	}
	// The header wants two signers, only the first one signed.
	two2 := append([]byte(nil), msg...)
	two2[0] = 2
	if tx, err := ParseTx(signed(priv, two2)); err != nil {
		t.Fatal(err)
	} else if err := tx.CheckBroadcastable(); err == nil || !strings.Contains(err.Error(), "wants 2") {
		t.Fatalf("1 signature for 2 signers: %v", err)
	}
	// Well formed and signed, but over the packet limit (a 1200-byte memo).
	big := append([]byte(nil), msg[:len(msg)-len(shortvec(12))-12]...)
	big = append(big, shortvec(1200)...)
	big = append(big, bytes.Repeat([]byte("m"), 1200)...)
	if raw := signed(priv, big); len(raw) <= MaxTxSize {
		t.Fatalf("test tx is only %d bytes", len(raw))
	} else if _, err := ParseTx(raw); err == nil || !strings.Contains(err.Error(), "at most 1232") {
		t.Fatalf("oversize: %v", err)
	}
	// Truncated, trailing bytes, unknown version, oversize, junk.
	good := signed(priv, msg)
	for name, b := range map[string][]byte{
		"truncated": good[:len(good)-1],
		"trailing":  append(append([]byte(nil), good...), 0),
		"version 1": signed(priv, append([]byte{0x81}, msg...)),
		"oversize":  append(good, make([]byte, MaxTxSize)...),
		"empty":     {},
	} {
		if _, err := ParseTx(b); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := ParseTxInput("not a transaction"); err == nil {
		t.Fatal("junk accepted")
	}
}

func TestLabel(t *testing.T) {
	key := "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
	for body, want := range map[string]string{
		`{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["` + key + `"]}`:                         "getBalance",
		`[{"method":"getBalance","params":["` + key + `"]},{"method":"getBalance"},{"method":"getSlot"}]`: "batch of 3 (getBalance x2, getSlot x1)",
		`{"method":"` + key + `"}`: "*",
		`{"method":"get balance"}`: "*",
		`not json`:                 "*",
		``:                         "(no body)",
		`{"method":"accountSubscribe","params":["x"]}`:                                 "accountSubscribe",
		`[{"method":"a"},{"method":"b"},{"method":"c"},{"method":"d"},{"method":"e"}]`: "batch of 5 (a x1, b x1, c x1, d x1, ...)",
	} {
		if got := Label([]byte(body)); got != want {
			t.Errorf("Label(%s) = %q, want %q", body, got, want)
		}
	}
}

func TestNetworks(t *testing.T) {
	r := NewRegistry()
	for _, n := range []string{"mainnet", "mainnet-beta", "MAINNET", "devnet", "testnet", "d"} {
		if _, ok := r.Get(n); !ok {
			t.Errorf("%s not found", n)
		}
	}
	m, _ := r.Get("mainnet")
	if len(m.Genesis) < 32 || m.CheckGenesis("EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG") == nil {
		t.Fatal("mainnet accepts devnet's genesis")
	}
	if m.CheckGenesis("5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d") != nil {
		t.Fatal("mainnet refuses its own genesis")
	}
	for _, bad := range []string{
		"x=http://plain.example", "x=https://user:pw@h.example", "x=https://h.example/?api-key=1", "x=https://h.example/#f",
		"x=https://h.example+ws://h.example", "x=https://h.example+https://h.example", "health=https://h.example",
		"mainnet-beta2!=https://h.example", "noequals", "x=",
	} {
		if err := r.ParseAdd(bad); err == nil {
			t.Errorf("--add %s accepted", bad)
		}
	}
	if err := r.ParseAdd("mine=https://a.example/rpc+wss://ws.example/x|https://b.example/"); err != nil {
		t.Fatal(err)
	}
	mine, _ := r.Get("mine")
	if mine.Genesis != "" || len(mine.Upstreams) != 2 || mine.Upstreams[0].WSURL() != "wss://ws.example/x" ||
		mine.Upstreams[1].WSURL() != "wss://b.example" || mine.Upstreams[1].RPC != "https://b.example" {
		t.Fatalf("mine: %+v", mine)
	}
	if mine.CheckGenesis("AAAA") != nil || mine.GenesisPinned() != "AAAA" || mine.CheckGenesis("BBBB") == nil {
		t.Fatal("custom network does not pin its first genesis")
	}
	// --add mainnet-beta=... replaces mainnet's list.
	if err := r.ParseAdd("mainnet-beta=https://my-node.example"); err != nil {
		t.Fatal(err)
	}
	if m, _ := r.Get("mainnet"); len(m.Upstreams) != 1 || m.Genesis == "" {
		t.Fatalf("mainnet: %+v", m)
	}
	if got := r.All(); len(got) != 4 || got[3].Name != "mine" {
		t.Fatalf("All: %v", got)
	}
	if Lamports(1_500_000_000) != "1.5 SOL" || Lamports(1) != "0.000000001 SOL" || Lamports(0) != "0 SOL" {
		t.Fatal(Lamports(1_500_000_000), Lamports(1), Lamports(0))
	}
}
