package aiacp

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestInitializeResponseFramePrecedesReverseRequest(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	handler := &initializeReadyHandler{ready: make(chan struct{})}
	server := ServeConn(serverEnd, handler)
	t.Cleanup(func() { _ = server.Close() })
	t.Cleanup(func() { _ = clientEnd.Close() })
	if err := clientEnd.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := clientEnd.Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n"))
		writeDone <- err
	}()
	reader := bufio.NewReader(clientEnd)
	readFrame := func() map[string]json.RawMessage {
		t.Helper()
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var frame map[string]json.RawMessage
		if err := json.Unmarshal(line, &frame); err != nil {
			t.Fatal(err)
		}
		return frame
	}
	first := readFrame()
	if string(first["id"]) != "1" || len(first["result"]) == 0 {
		t.Fatalf("first frame must be initialize response: %v", first)
	}
	second := readFrame()
	if string(second["method"]) != `"x-device/tools"` {
		t.Fatalf("second frame must be the reverse request: %v", second)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}
