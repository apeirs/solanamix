package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/apeirs/solanamix/sol"
	"github.com/apeirs/solanamix/wsx"
)

// subscribe opens the network's PubSub websocket over the mixnet, sends one
// subscription and calls fn with each notification's result until fn
// returns false or ctx ends.
func subscribe(ctx context.Context, cl *sol.Client, n *sol.Network, method string, params []any, fn func(json.RawMessage) bool) error {
	ws, err := cl.DialWS(ctx, n)
	if err != nil {
		return err
	}
	defer ws.Close()
	stop := context.AfterFunc(ctx, func() { _ = ws.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	req := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method}
	if params != nil {
		req["params"] = params
	}
	b, _ := json.Marshal(req)
	if err := wsx.WriteFrame(ws, wsx.OpText, b, true); err != nil {
		return err
	}
	var msg []byte
	for {
		f, err := wsx.ReadFrame(ws.Reader)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("websocket from %s: %w", sol.HostOf(ws.Upstream), err)
		}
		switch f.Op {
		case wsx.OpPing:
			_ = wsx.WriteFrame(ws, wsx.OpPong, f.Payload, true)
			continue
		case wsx.OpClose:
			return fmt.Errorf("websocket closed by %s", sol.HostOf(ws.Upstream))
		case wsx.OpText, wsx.OpCont:
			msg = append(msg, f.Payload...)
		default:
			continue
		}
		if !f.Fin {
			continue
		}
		var m struct {
			ID     *int            `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *sol.RPCError   `json:"error"`
			Params struct {
				Result json.RawMessage `json:"result"`
			} `json:"params"`
		}
		err = json.Unmarshal(msg, &m)
		msg = nil
		if err != nil {
			continue
		}
		if m.Error != nil {
			return m.Error
		}
		if m.ID != nil {
			continue // the subscription id
		}
		if !fn(m.Params.Result) {
			_ = wsx.WriteFrame(ws, wsx.OpClose, []byte{0x03, 0xe8}, true)
			return nil
		}
	}
}

func cmdWatch(args []string) {
	var c common
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	c.register(fs)
	count := fs.Int("count", 5, "stop after this many notifications (0 = run until ^C)")
	pos := parse(fs, args)
	if len(pos) < 1 {
		die(errors.New("usage: solanamix watch slot | account PUBKEY | signature SIG [--count N]"))
	}
	var method string
	var params []any
	switch {
	case pos[0] == "slot" && len(pos) == 1:
		method = "slotSubscribe"
	case pos[0] == "account" && len(pos) == 2:
		if _, err := sol.ParsePubkey(pos[1]); err != nil {
			die(err)
		}
		method, params = "accountSubscribe", []any{pos[1], map[string]string{"encoding": "jsonParsed", "commitment": "confirmed"}}
	case pos[0] == "signature" && len(pos) == 2:
		method, params = "signatureSubscribe", []any{pos[1], map[string]string{"commitment": "confirmed"}}
	default:
		die(errors.New("usage: solanamix watch slot | account PUBKEY | signature SIG [--count N]"))
	}
	ctx, cancel := c.ctx()
	defer cancel()
	n := c.net()
	t0 := time.Now()
	seen := 0
	err := subscribe(ctx, c.client(), n, method, params, func(v json.RawMessage) bool {
		seen++
		if c.jsonOut {
			fmt.Println(string(v))
		} else if method == "slotSubscribe" {
			var s struct {
				Slot, Parent, Root uint64
			}
			_ = json.Unmarshal(v, &s)
			fmt.Printf("[%5.1fs] %s slot %d (parent %d, root %d)\n", time.Since(t0).Seconds(), n.Name, s.Slot, s.Parent, s.Root)
		} else {
			fmt.Printf("[%5.1fs] %s %s: %s\n", time.Since(t0).Seconds(), n.Name, method, v)
		}
		return *count == 0 || seen < *count
	})
	if err != nil && !(errors.Is(err, context.Canceled) && seen > 0) {
		die(err)
	}
	fmt.Fprintf(os.Stderr, "%d notification(s) over the mixnet PubSub websocket in %.1fs\n", seen, time.Since(t0).Seconds())
}
