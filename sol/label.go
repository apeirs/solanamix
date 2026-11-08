package sol

import (
	"encoding/json"
	"fmt"
	"strings"
)

func validMethod(s string) bool {
	if s == "" || len(s) > 48 {
		return false
	}
	return strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") == ""
}

// Label names the JSON-RPC method(s) of a request body for logs: "getBalance"
// or "batch of 3 (getBalance x2, getSlot x1)". Params (addresses,
// signatures, transactions) never appear. A method name that looks like a
// base58 key or signature is shown as "*".
func Label(body []byte) string {
	if len(body) == 0 {
		return "(no body)"
	}
	name := func(raw json.RawMessage) string {
		var m struct {
			Method string `json:"method"`
		}
		if json.Unmarshal(raw, &m) != nil || !validMethod(m.Method) || looksLikeKey(m.Method) {
			return "*"
		}
		return m.Method
	}
	var batch []json.RawMessage
	if json.Unmarshal(body, &batch) == nil {
		counts := map[string]int{}
		var order []string
		for _, r := range batch {
			n := name(r)
			if counts[n] == 0 {
				order = append(order, n)
			}
			counts[n]++
		}
		var parts []string
		for i, n := range order {
			if i == 4 {
				parts = append(parts, "...")
				break
			}
			parts = append(parts, fmt.Sprintf("%s x%d", n, counts[n]))
		}
		return fmt.Sprintf("batch of %d (%s)", len(batch), strings.Join(parts, ", "))
	}
	return name(body)
}

// looksLikeKey: a 32+ character base58 string with digits and both cases
// is a key, hash or signature, not a method name.
func looksLikeKey(s string) bool {
	if len(s) < 32 || strings.Trim(s, b58) != "" {
		return false
	}
	return strings.ContainsAny(s, "123456789")
}
