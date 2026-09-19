package protocol

import (
	"context"
	"encoding/json"
	"errors"

	"net"
	"testing"
	"time"
)

func pipeConns(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	a, b := net.Pipe()
	connA, connB := NewConn(a), NewConn(b)
	go func() { _ = connA.Serve() }()
	go func() { _ = connB.Serve() }()
	t.Cleanup(func() {
		_ = connA.Close()
		_ = connB.Close()
	})
	return connA, connB
}

func TestConn_CallRoundtrip(t *testing.T) {
	server, client := pipeConns(t)
	server.Handle("echo", func(_ context.Context, params json.RawMessage) (any, error) {
		var req struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, err
		}
		return map[string]string{"echo": req.Text}, nil
	})

	var resp struct {
		Echo string `json:"echo"`
	}
	err := client.Call(context.Background(), "echo", map[string]string{"text": "ping"}, &resp)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Echo != "ping" {
		t.Errorf("echo = %q, want ping", resp.Echo)
	}
}

func TestConn_CallErrorPropagates(t *testing.T) {
	server, client := pipeConns(t)
	server.Handle("boom", func(context.Context, json.RawMessage) (any, error) {
		return nil, &RPCError{Code: 42, Message: "custom failure"}
	})

	err := client.Call(context.Background(), "boom", nil, nil)
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *RPCError", err)
	}
	if rpcErr.Code != 42 || rpcErr.Message != "custom failure" {
		t.Errorf("rpc error = %v", rpcErr)
	}
}

func TestConn_MethodNotFound(t *testing.T) {
	_, client := pipeConns(t)
	err := client.Call(context.Background(), "missing", nil, nil)
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32601 {
		t.Fatalf("err = %v, want method-not-found RPCError", err)
	}
}

func TestConn_Notification(t *testing.T) {
	server, client := pipeConns(t)
	got := make(chan json.RawMessage, 1)
	server.HandleNotification("event", func(params json.RawMessage) {
		got <- params
	})

	if err := client.Notify("event", map[string]int{"n": 1}); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	select {
	case params := <-got:
		var payload struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(params, &payload); err != nil || payload.N != 1 {
			t.Errorf("notification payload = %s", params)
		}
	case <-time.After(time.Second):
		t.Fatal("notification not delivered")
	}
}

func TestConn_CallAfterClose(t *testing.T) {
	_, client := pipeConns(t)
	_ = client.Close()
	if err := client.Call(context.Background(), "anything", nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestConn_CloseUnblocksPendingCall(t *testing.T) {
	server, client := pipeConns(t)
	server.Handle("slow", func(ctx context.Context, _ json.RawMessage) (any, error) {
		<-ctx.Done() // 模拟挂住的 handler
		return nil, ctx.Err()
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Call(context.Background(), "slow", nil, nil)
	}()
	time.Sleep(50 * time.Millisecond) // 等 Call 落到 pending
	_ = client.Close()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("pending call err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call not unblocked by Close")
	}
}

func TestConn_ConcurrentCalls(t *testing.T) {
	server, client := pipeConns(t)
	server.Handle("id", func(_ context.Context, params json.RawMessage) (any, error) {
		var n int
		if err := json.Unmarshal(params, &n); err != nil {
			return nil, err
		}
		return n * 2, nil
	})

	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			var got int
			if err := client.Call(context.Background(), "id", n, &got); err != nil {
				errs <- err
				return
			}
			if got != n*2 {
				errs <- errors.New("mismatched response: request/response crossed")
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < 10; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestConn_IgnoresNonJSONLines(t *testing.T) {
	rawA, rawB := net.Pipe()
	server := NewConn(rawA)
	go func() { _ = server.Serve() }()

	got := make(chan struct{}, 1)
	server.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		got <- struct{}{}
		return "pong", nil
	})

	// 手工写：垃圾行 + 合法请求，服务器应跳过前者
	garbage := []byte("this is not json at all\n")
	request := []byte(`{"jsonrpc":"2.0","id":7,"method":"ping"}` + "\n")
	go func() {
		_, _ = rawB.Write(garbage)
		_, _ = rawB.Write(request)
	}()

	respData := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 512)
		n, _ := rawB.Read(buf)
		respData <- buf[:n]
	}()

	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("handler not called after garbage line")
	}
	select {
	case resp := <-respData:
		if string(resp) != `{"jsonrpc":"2.0","id":7,"result":"pong"}`+"\n" {
			t.Errorf("response = %q", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("no response received")
	}
	_ = server.Close()
	_ = rawB.Close()
}
