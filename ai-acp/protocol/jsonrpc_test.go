package protocol

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
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

func TestNewConnWithOptions(t *testing.T) {
	if _, err := NewConnWithOptions(nil, ConnOptions{}); err == nil {
		t.Fatal("zero message limit should fail")
	}
	if _, err := NewConnWithOptions(nil, ConnOptions{MaxMessageBytes: -1}); err == nil {
		t.Fatal("negative message limit should fail")
	}
	if _, err := NewConnWithOptions(nil, ConnOptions{MaxMessageBytes: 42}); err == nil {
		t.Fatal("nil transport should fail")
	}
	rawServer, rawClient := net.Pipe()
	t.Cleanup(func() { _ = rawClient.Close() })
	conn, err := NewConnWithOptions(rawServer, ConnOptions{MaxMessageBytes: 42})
	if err != nil {
		t.Fatalf("NewConnWithOptions: %v", err)
	}
	if conn.maxMessageBytes != 42 {
		t.Fatalf("max message bytes = %d, want 42", conn.maxMessageBytes)
	}
}

func TestNewConnNilTransportPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewConn(nil) should panic")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic value = %v, want error", r)
		}
		if err.Error() != "acp: nil transport" {
			t.Fatalf("panic error = %v, want acp: nil transport", err)
		}
	}()
	NewConn(nil)
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
	if err := client.Call(context.Background(), "echo", map[string]string{"text": "ping"}, &resp); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Echo != "ping" {
		t.Errorf("echo = %q, want ping", resp.Echo)
	}
}

func TestConn_RequestHandlerPanicReturnsInternalError(t *testing.T) {
	server, client := pipeConns(t)
	server.Handle("boom", func(context.Context, json.RawMessage) (any, error) {
		panic("handler exploded")
	})
	server.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		return "pong", nil
	})

	var rpcErr *RPCError
	if err := client.Call(context.Background(), "boom", nil, nil); !errors.As(err, &rpcErr) || rpcErr.Code != CodeInternalError {
		t.Fatalf("err = %v, want internal-error RPCError", err)
	}
	var result string
	if err := client.Call(context.Background(), "ping", nil, &result); err != nil || result != "pong" {
		t.Fatalf("post-panic Call = %q, %v; connection should stay usable", result, err)
	}
}

func TestConn_NotificationHandlerPanicDoesNotStallFIFO(t *testing.T) {
	server, client := pipeConns(t)
	got := make(chan int, 2)
	server.HandleNotification("event", func(params json.RawMessage) {
		var payload struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(params, &payload); err != nil {
			t.Errorf("notification payload: %v", err)
			return
		}
		if payload.N == 1 {
			panic("first notification exploded")
		}
		got <- payload.N
	})

	if err := client.Notify("event", map[string]int{"n": 1}); err != nil {
		t.Fatalf("first Notify: %v", err)
	}
	if err := client.Notify("event", map[string]int{"n": 2}); err != nil {
		t.Fatalf("second Notify: %v", err)
	}
	select {
	case n := <-got:
		if n != 2 {
			t.Fatalf("delivered notification = %d, want 2", n)
		}
	case <-time.After(time.Second):
		t.Fatal("notification after a panicking one was not delivered; FIFO chain stalled")
	}
}

func TestConn_CallErrorPropagates(t *testing.T) {
	server, client := pipeConns(t)
	server.Handle("boom", func(context.Context, json.RawMessage) (any, error) {
		return nil, &RPCError{Code: 42, Message: "custom failure"}
	})

	var rpcErr *RPCError
	err := client.Call(context.Background(), "boom", nil, nil)
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *RPCError", err)
	}
	if rpcErr.Code != 42 || rpcErr.Message != "custom failure" {
		t.Fatalf("rpc error = %v", rpcErr)
	}
}

func TestConn_MethodNotFound(t *testing.T) {
	_, client := pipeConns(t)
	err := client.Call(context.Background(), "missing", nil, nil)
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != CodeMethodNotFound {
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

func TestConn_NotificationFIFO(t *testing.T) {
	server, client := pipeConns(t)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	got := make(chan int, 2)
	server.HandleNotification("event", func(params json.RawMessage) {
		var payload struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(params, &payload); err != nil {
			t.Errorf("notification payload: %v", err)
			return
		}
		if payload.N == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		got <- payload.N
	})

	if err := client.Notify("event", map[string]int{"n": 1}); err != nil {
		t.Fatalf("first Notify: %v", err)
	}
	<-firstStarted
	if err := client.Notify("event", map[string]int{"n": 2}); err != nil {
		t.Fatalf("second Notify: %v", err)
	}
	select {
	case n := <-got:
		t.Fatalf("notification %d ran before the first completed", n)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseFirst)
	if first, second := <-got, <-got; first != 1 || second != 2 {
		t.Fatalf("notification order = %d, %d; want 1, 2", first, second)
	}
}

func TestConn_CallAfterClose(t *testing.T) {
	_, client := pipeConns(t)
	_ = client.Close()
	if err := client.Call(context.Background(), "anything", nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestConn_CloseCancelsRunningHandler(t *testing.T) {
	server, client := pipeConns(t)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	server.Handle("slow", func(ctx context.Context, _ json.RawMessage) (any, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	})

	errCh := make(chan error, 1)
	go func() { errCh <- client.Call(context.Background(), "slow", nil, nil) }()
	<-started
	_ = client.Close()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("handler was not cancelled")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Call error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending Call was not unblocked")
	}
}

func TestConn_ReportsProtocolErrorsAndContinues(t *testing.T) {
	rawServer, rawClient := net.Pipe()
	server := NewConn(rawServer)
	go func() { _ = server.Serve() }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = rawClient.Close()
	})

	server.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		return "pong", nil
	})
	reader := bufio.NewReader(rawClient)
	cases := []struct {
		name string
		line string
		code int
		id   string
	}{
		{name: "parse error", line: "not-json", code: CodeParseError, id: "null"},
		{name: "wrong version", line: `{"jsonrpc":"1.0","id":1,"method":"ping"}`, code: CodeInvalidRequest, id: "1"},
		{name: "missing method", line: `{"jsonrpc":"2.0","id":2}`, code: CodeInvalidRequest, id: "2"},
		{name: "request and result", line: `{"jsonrpc":"2.0","id":3,"method":"ping","result":null}`, code: CodeInvalidRequest, id: "3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeLine(t, rawClient, tc.line)
			var response Response
			readLine(t, reader, &response)
			if response.ID == nil || string(response.ID) != tc.id {
				t.Fatalf("response id = %s, want %s", response.ID, tc.id)
			}
			if response.Error == nil || response.Error.Code != tc.code {
				t.Fatalf("response error = %+v, want code %d", response.Error, tc.code)
			}
		})
	}

	writeLine(t, rawClient, `{"jsonrpc":"2.0","id":4,"method":"ping"}`)
	var response Response
	readLine(t, reader, &response)
	if string(response.Result) != `"pong"` {
		t.Fatalf("result = %s, want pong", response.Result)
	}
}

func TestConn_MaxMessageBytes(t *testing.T) {
	payload := `{"jsonrpc":"2.0","method":"event"}`
	t.Run("exact limit", func(t *testing.T) {
		rawServer, rawClient := net.Pipe()
		server, err := NewConnWithOptions(rawServer, ConnOptions{MaxMessageBytes: len(payload)})
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = server.Serve() }()
		t.Cleanup(func() {
			_ = server.Close()
			_ = rawClient.Close()
		})
		called := make(chan struct{})
		server.HandleNotification("event", func(json.RawMessage) { close(called) })
		writeLine(t, rawClient, payload)
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("exact-limit frame was rejected")
		}
	})

	t.Run("over limit", func(t *testing.T) {
		rawServer, rawClient := net.Pipe()
		server, err := NewConnWithOptions(rawServer, ConnOptions{MaxMessageBytes: len(payload)})
		if err != nil {
			t.Fatal(err)
		}
		serveErr := make(chan error, 1)
		go func() { serveErr <- server.Serve() }()
		t.Cleanup(func() {
			_ = server.Close()
			_ = rawClient.Close()
		})
		called := make(chan struct{}, 1)
		server.HandleNotification("event", func(json.RawMessage) { called <- struct{}{} })
		writeLine(t, rawClient, payload+" ")
		select {
		case err := <-serveErr:
			if !errors.Is(err, ErrMessageTooLarge) {
				t.Fatalf("Serve error = %v, want ErrMessageTooLarge", err)
			}
		case <-time.After(time.Second):
			t.Fatal("oversized frame did not terminate Serve")
		}
		select {
		case <-called:
			t.Fatal("oversized frame reached handler")
		default:
		}
	})
}

func TestConn_ShortWriteTerminatesConnection(t *testing.T) {
	conn := NewConn(&shortWriter{})
	err := conn.Notify("event", nil)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Notify error = %v, want io.ErrShortWrite", err)
	}
	if !errors.Is(conn.ServeErr(), io.ErrShortWrite) {
		t.Fatalf("ServeErr = %v, want io.ErrShortWrite", conn.ServeErr())
	}
}

type shortWriter struct{}

func (*shortWriter) Read([]byte) (int, error)    { return 0, io.EOF }
func (*shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func (*shortWriter) Close() error                { return nil }

func writeLine(t *testing.T, conn io.Writer, line string) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := conn.Write(append([]byte(line), '\n'))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write line: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write line timed out")
	}
}

func readLine(t *testing.T, reader *bufio.Reader, response *Response) {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := json.Unmarshal(line, response); err != nil {
		t.Fatalf("decode response %s: %v", line, err)
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
				return
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

func TestConn_NotificationHandlerDoesNotBlockReader(t *testing.T) {
	server, client := pipeConns(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server.HandleNotification("slow", func(json.RawMessage) {
		once.Do(func() { close(started) })
		<-release
	})
	if err := client.Notify("slow", nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := client.Notify("slow", nil); err != nil {
		t.Fatal(err)
	}
	close(release)
}
