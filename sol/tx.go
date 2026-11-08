package sol

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

const b58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// Base58 encodes b with the Bitcoin alphabet (Solana keys, hashes, signatures).
func Base58(b []byte) string {
	n := new(big.Int).SetBytes(b)
	var out []byte
	mod, base := new(big.Int), big.NewInt(58)
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append(out, b58[mod.Int64()])
	}
	for _, c := range b {
		if c != 0 {
			break
		}
		out = append(out, '1')
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

// DecodeBase58 is the inverse of Base58.
func DecodeBase58(s string) ([]byte, error) {
	n := new(big.Int)
	base := big.NewInt(58)
	for _, c := range []byte(s) {
		i := strings.IndexByte(b58, c)
		if i < 0 {
			return nil, fmt.Errorf("invalid base58 character %q", c)
		}
		n.Mul(n, base)
		n.Add(n, big.NewInt(int64(i)))
	}
	out := n.Bytes()
	zeros := 0
	for zeros < len(s) && s[zeros] == '1' {
		zeros++
	}
	return append(make([]byte, zeros), out...), nil
}

// ParsePubkey decodes a base58 public key (32 bytes).
func ParsePubkey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if len(s) < 32 || len(s) > 44 {
		return nil, fmt.Errorf("%q is not a Solana address (32 bytes in base58)", s)
	}
	b, err := DecodeBase58(s)
	if err != nil || len(b) != 32 {
		return nil, fmt.Errorf("%q is not a Solana address (32 bytes in base58)", s)
	}
	return b, nil
}

// Tx is a parsed wire transaction (legacy or v0).
type Tx struct {
	Signatures   [][]byte
	Message      []byte // the signed bytes
	Version      int    // -1 = legacy
	NumRequired  int
	Keys         [][]byte // static account keys
	Blockhash    []byte
	Instructions int
	Lookups      int // v0 address table lookups
	raw          []byte
}

// MaxTxSize is the packet limit the cluster enforces for a serialized
// transaction.
const MaxTxSize = 1232

type reader struct {
	b   []byte
	off int
}

var errShort = errors.New("not a Solana transaction: truncated")

func (r *reader) byte() (byte, error) {
	if r.off >= len(r.b) {
		return 0, errShort
	}
	c := r.b[r.off]
	r.off++
	return c, nil
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || r.off+n > len(r.b) {
		return nil, errShort
	}
	out := r.b[r.off : r.off+n]
	r.off += n
	return out, nil
}

// compact reads a compact-u16 (shortvec) length.
func (r *reader) compact() (int, error) {
	v := 0
	for i := 0; i < 3; i++ {
		c, err := r.byte()
		if err != nil {
			return 0, err
		}
		v |= int(c&0x7f) << (7 * i)
		if c&0x80 == 0 {
			if i > 0 && c == 0 {
				return 0, errors.New("not a Solana transaction: non-canonical length")
			}
			return v, nil
		}
	}
	return 0, errors.New("not a Solana transaction: bad length")
}

func (r *reader) bytesList() (int, error) {
	n, err := r.compact()
	if err != nil {
		return 0, err
	}
	_, err = r.take(n)
	return n, err
}

// ParseTx parses a serialized transaction. Every byte must belong to it.
func ParseTx(b []byte) (*Tx, error) {
	if len(b) > MaxTxSize {
		return nil, fmt.Errorf("transaction is %d bytes; the cluster takes at most %d", len(b), MaxTxSize)
	}
	r := &reader{b: b}
	ns, err := r.compact()
	if err != nil {
		return nil, err
	}
	t := &Tx{Version: -1, raw: b}
	for i := 0; i < ns; i++ {
		s, err := r.take(64)
		if err != nil {
			return nil, err
		}
		t.Signatures = append(t.Signatures, s)
	}
	msgStart := r.off
	first, err := r.byte()
	if err != nil {
		return nil, err
	}
	if first&0x80 != 0 {
		t.Version = int(first & 0x7f)
		if t.Version != 0 {
			return nil, fmt.Errorf("transaction message version %d is not supported (legacy and v0 are)", t.Version)
		}
		if first, err = r.byte(); err != nil {
			return nil, err
		}
	}
	t.NumRequired = int(first)
	if _, err := r.take(2); err != nil { // read-only signed, read-only unsigned
		return nil, err
	}
	nk, err := r.compact()
	if err != nil {
		return nil, err
	}
	for i := 0; i < nk; i++ {
		k, err := r.take(32)
		if err != nil {
			return nil, err
		}
		t.Keys = append(t.Keys, k)
	}
	if t.Blockhash, err = r.take(32); err != nil {
		return nil, err
	}
	if t.Instructions, err = r.compact(); err != nil {
		return nil, err
	}
	for i := 0; i < t.Instructions; i++ {
		if _, err := r.byte(); err != nil { // program id index
			return nil, err
		}
		if _, err := r.bytesList(); err != nil { // account indexes
			return nil, err
		}
		if _, err := r.bytesList(); err != nil { // data
			return nil, err
		}
	}
	if t.Version == 0 {
		if t.Lookups, err = r.compact(); err != nil {
			return nil, err
		}
		for i := 0; i < t.Lookups; i++ {
			if _, err := r.take(32); err != nil {
				return nil, err
			}
			if _, err := r.bytesList(); err != nil {
				return nil, err
			}
			if _, err := r.bytesList(); err != nil {
				return nil, err
			}
		}
	}
	if r.off != len(b) {
		return nil, fmt.Errorf("not a Solana transaction: %d unexpected bytes at the end", len(b)-r.off)
	}
	t.Message = b[msgStart:]
	return t, nil
}

// ID is the transaction's id: its first signature in base58.
func (t *Tx) ID() string {
	if len(t.Signatures) == 0 {
		return ""
	}
	return Base58(t.Signatures[0])
}

// Bytes is the serialized transaction.
func (t *Tx) Bytes() []byte { return t.raw }

// Signer is the fee payer (first account key).
func (t *Tx) Signer() string {
	if len(t.Keys) == 0 {
		return ""
	}
	return Base58(t.Keys[0])
}

// CheckBroadcastable refuses a transaction the cluster would reject for
// reasons visible locally: missing or invalid signatures. solanamix holds no
// keys and only forwards what was signed elsewhere.
func (t *Tx) CheckBroadcastable() error {
	if t.NumRequired == 0 || len(t.Keys) < t.NumRequired {
		return fmt.Errorf("transaction wants %d signers but lists %d account keys", t.NumRequired, len(t.Keys))
	}
	if len(t.Signatures) != t.NumRequired {
		return fmt.Errorf("transaction has %d signature slots, its message wants %d", len(t.Signatures), t.NumRequired)
	}
	for i, s := range t.Signatures {
		zero := true
		for _, c := range s {
			if c != 0 {
				zero = false
				break
			}
		}
		if zero {
			return fmt.Errorf("transaction is not signed by %s (signature %d of %d is empty): solanamix only forwards signed transactions", Base58(t.Keys[i]), i+1, len(t.Signatures))
		}
		if !ed25519.Verify(ed25519.PublicKey(t.Keys[i]), t.Message, s) {
			return fmt.Errorf("signature %d of %d does not verify for %s (the message was changed after signing, or signed by another key)", i+1, len(t.Signatures), Base58(t.Keys[i]))
		}
	}
	return nil
}

// ParseTxInput reads a serialized transaction given as base64 (what
// web3.js serialize().toString('base64') and the CLI's --dump-transaction
// print) or base58.
func ParseTxInput(s string) (*Tx, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, errors.New("empty transaction")
	}
	var errs []error
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		t, err := ParseTx(b)
		if err == nil {
			return t, nil
		}
		errs = append(errs, fmt.Errorf("as base64: %w", err))
	}
	if strings.Trim(s, b58) == "" {
		if b, err := DecodeBase58(s); err == nil {
			t, err := ParseTx(b)
			if err == nil {
				return t, nil
			}
			errs = append(errs, fmt.Errorf("as base58: %w", err))
		}
	}
	if len(errs) == 0 {
		return nil, errors.New("not base64 or base58")
	}
	return nil, errors.Join(errs...)
}

// Lamports formats lamports as SOL.
func Lamports(l uint64) string {
	s := fmt.Sprintf("%d.%09d", l/1e9, l%1e9)
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".") + " SOL"
}
