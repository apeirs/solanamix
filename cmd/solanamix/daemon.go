package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/apeirs/solanamix/nym"
)

// ---------------------------------------------------------------- start / stop / status
//
// `solanamix start` = `solanamix nym setup` (first time) or `nym start`, then
// `solanamix serve` in the background. `solanamix stop` stops both.

type gwState struct {
	PID      int    `json:"pid"`
	Listen   string `json:"listen"`
	ListenWS string `json:"listen_ws"`
	Default  string `json:"default"`
	Proxy    string `json:"proxy"`
	Log      string `json:"log"`
}

func gwStateFile(id string) string { return filepath.Join(nym.StateDir(), id+"-gateway.json") }

func readGW(id string) (gwState, bool) {
	var st gwState
	b, err := os.ReadFile(gwStateFile(id))
	if err != nil || json.Unmarshal(b, &st) != nil || st.PID <= 0 {
		return st, false
	}
	return st, syscall.Kill(st.PID, 0) == nil
}

func cmdStart(args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	binary := fs.String("binary", "", "path to nym-socks5-client ($SOLANAMIX_NYM_SOCKS5)")
	id := fs.String("id", nym.DefaultID, "client id (~/.nym/socks5-clients/ID)")
	port := fs.Int("port", nym.DefaultPort, "local SOCKS5 port (used when the client is set up the first time)")
	minPerf := fs.Float64("min-performance", 0.95, "minimum exit performance when picking the exit gateway")
	listen := fs.String("listen", "127.0.0.1:8899", "gateway JSON-RPC address")
	listenWS := fs.String("listen-ws", "127.0.0.1:8900", "gateway PubSub address")
	def := fs.String("network", "mainnet", "network served at the gateway root (all are under /<name>)")
	var add multi
	fs.Var(&add, "add", "network upstreams for the gateway: name=https://rpc|https://rpc2 (repeatable)")
	parse(fs, args)
	bin, err := nym.FindBinary(*binary)
	if err != nil {
		die(err)
	}
	if err := setupNym(bin, *id, "auto", *port, true, "", false, *minPerf); err != nil {
		die(err)
	}
	st, err := nym.Start(bin, *id, 3*time.Minute)
	if err != nil {
		die(err)
	}
	fmt.Printf("mixnet client   %s, SOCKS5 %s (pid %d)\n", st.ID, st.SOCKS5, st.PID)
	if gs, ok := readGW(*id); ok {
		fmt.Printf("gateway         already running (pid %d) on http://%s and ws://%s\n", gs.PID, gs.Listen, gs.ListenWS)
		return
	}
	exe, err := os.Executable()
	if err != nil {
		die(err)
	}
	sargs := []string{"serve", "--proxy", st.SOCKS5, "--listen", *listen, "--listen-ws", *listenWS, "--default", *def}
	for _, a := range add {
		sargs = append(sargs, "--add", a)
	}
	logPath := filepath.Join(nym.StateDir(), *id+"-gateway.log")
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		die(err)
	}
	cmd := exec.Command(exe, sargs...)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		die(err)
	}
	gs := gwState{PID: cmd.Process.Pid, Listen: *listen, ListenWS: *listenWS, Default: *def, Proxy: st.SOCKS5, Log: logPath}
	b, _ := json.Marshal(gs)
	_ = os.WriteFile(gwStateFile(*id), b, 0o600)
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-exited:
			_ = os.Remove(gwStateFile(*id))
			die(fmt.Errorf("the gateway exited at once; see %s", logPath))
		case <-time.After(200 * time.Millisecond):
		}
		if c, err := net.DialTimeout("tcp", *listen, time.Second); err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			die(fmt.Errorf("the gateway is not listening on %s after 10s; see %s", *listen, logPath))
		}
	}
	fmt.Printf("gateway         pid %d, log %s\n", gs.PID, logPath)
	fmt.Printf("  JSON-RPC      http://%s (%s)   http://%s/devnet   http://%s/testnet\n", *listen, *def, *listen, *listen)
	if *listenWS != "" {
		fmt.Printf("  PubSub        ws://%s (%s)     ws://%s/devnet     ws://%s/testnet\n", *listenWS, *def, *listenWS, *listenWS)
	}
	fmt.Printf("use it:         solana config set --url http://%s\n", *listen)
}

func cmdStop(args []string) {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	id := fs.String("id", nym.DefaultID, "client id")
	parse(fs, args)
	if gs, ok := readGW(*id); ok {
		_ = syscall.Kill(gs.PID, syscall.SIGTERM)
		fmt.Printf("gateway stopped (pid %d)\n", gs.PID)
	} else {
		fmt.Println("gateway not running")
	}
	_ = os.Remove(gwStateFile(*id))
	if nym.Stop(*id) {
		fmt.Println("mixnet client stopped")
	} else {
		fmt.Println("mixnet client not running")
	}
}

func cmdStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	id := fs.String("id", nym.DefaultID, "client id")
	parse(fs, args)
	st, err := nym.GetStatus(*id)
	if err != nil {
		die(err)
	}
	pid := "not running"
	if st.PID != 0 {
		pid = "pid " + strconv.Itoa(st.PID)
	}
	fmt.Printf("mixnet client   %s, SOCKS5 %s, %s\n", st.ID, st.SOCKS5, pid)
	if st.Gateway != "" {
		fmt.Printf("  entry gateway %s\n  exit          %s\n", st.Gateway, shorten(st.Provider))
	}
	if gs, ok := readGW(*id); ok {
		fmt.Printf("gateway         pid %d on http://%s and ws://%s (root = %s), log %s\n", gs.PID, gs.Listen, gs.ListenWS, gs.Default, gs.Log)
	} else {
		fmt.Println("gateway         not running (solanamix start, or solanamix serve in the foreground)")
	}
}

func shorten(s string) string {
	if len(s) > 40 {
		return s[:16] + "..." + s[strings.LastIndex(s, "@"):]
	}
	return s
}
