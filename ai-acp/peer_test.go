package aiacp

import (
	"errors"
	"io"
	"sync"
	"testing"
)

// recordingTransport 记录 Close 次数，供并发关闭测试断言 exactly-once。
type recordingTransport struct {
	mu       sync.Mutex
	closeErr error
	closes   int
}

func (*recordingTransport) Read([]byte) (int, error)    { return 0, io.EOF }
func (*recordingTransport) Write(p []byte) (int, error) { return len(p), nil }

func (t *recordingTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closes++
	return t.closeErr
}

func (t *recordingTransport) closeCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closes
}

// hookRecorder 记录 close hook 的调用次数与返回错误。
type hookRecorder struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (h *hookRecorder) run() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	return h.err
}

func (h *hookRecorder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// TestPeerCloseClosesTransportAndRunsCloseFn 通过 newPeer 的 hook 路径验证
// closingTransport.Close：内嵌 transport 与 close hook 各执行一次，错误合并。
func TestPeerCloseClosesTransportAndRunsCloseFn(t *testing.T) {
	errTransport := errors.New("transport close failed")
	errHook := errors.New("close hook failed")

	tests := []struct {
		name         string
		useHook      bool
		transportErr error
		hookErr      error
		wantIs       []error
	}{
		{
			name: "no hook: embedded transport still closed",
		},
		{
			name:    "hook path: transport and hook each invoked once",
			useHook: true,
		},
		{
			name:         "transport close error propagates",
			useHook:      true,
			transportErr: errTransport,
			wantIs:       []error{errTransport},
		},
		{
			name:    "hook close error propagates",
			useHook: true,
			hookErr: errHook,
			wantIs:  []error{errHook},
		},
		{
			name:         "both fail: errors joined",
			useHook:      true,
			transportErr: errTransport,
			hookErr:      errHook,
			wantIs:       []error{errTransport, errHook},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := &recordingTransport{closeErr: tc.transportErr}
			hook := &hookRecorder{err: tc.hookErr}

			newPeerFn := newPeer
			if !tc.useHook {
				newPeerFn = func(rwc io.ReadWriteCloser, fn func() error) (*Peer, error) {
					return NewPeer(rwc)
				}
			}

			peer, err := newPeerFn(transport, hook.run)
			if err != nil {
				t.Fatalf("newPeer: %v", err)
			}

			// 并发 Close，验证 exactly-once 语义。
			const goroutines = 8
			var wg sync.WaitGroup
			errs := make([]error, goroutines)
			for i := 0; i < goroutines; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					errs[i] = peer.Close()
				}(i)
			}
			wg.Wait()

			if got := transport.closeCount(); got != 1 {
				t.Errorf("transport closes = %d, want 1", got)
			}
			if tc.useHook {
				if got := hook.count(); got != 1 {
					t.Errorf("hook calls = %d, want 1", got)
				}
			}
			// closeOnce 语义：只有执行关闭的那个调用者拿到错误，其余返回 nil。
			var joined error
			nonNil := 0
			for _, err := range errs {
				if err != nil {
					nonNil++
					joined = err
				}
			}
			if len(tc.wantIs) == 0 {
				if nonNil != 0 {
					t.Errorf("Close() errors = %v, want all nil", errs)
				}
				return
			}
			if nonNil != 1 {
				t.Errorf("non-nil Close() errors = %d, want exactly 1", nonNil)
			}
			if joined != nil {
				for _, want := range tc.wantIs {
					if !errors.Is(joined, want) {
						t.Errorf("Close() error = %v, want wrapping %v", joined, want)
					}
				}
			}
		})
	}
}
