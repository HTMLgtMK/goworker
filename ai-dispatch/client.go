package dispatch

import (
	"context"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
)

// WorkerSpec describes an ACP worker launched as a child process. Process
// management belongs to dispatch; the protocol client itself lives in ai-acp.
type WorkerSpec struct {
	Name    string   `json:"name"`
	Command string   `json:"command"`
	Args    []string `json:"args,omitempty"`
	Env     []string `json:"env,omitempty"`
}

// Opener abstracts worker connection setup for tests and embedding hosts.
type Opener func(ctx context.Context, spec WorkerSpec) (io.ReadWriteCloser, io.Closer, error)

// ProcessOpener starts a worker with stdin/stdout as its ACP transport.
func ProcessOpener(ctx context.Context, spec WorkerSpec) (io.ReadWriteCloser, io.Closer, error) {
	cmd := exec.CommandContext(ctx, spec.Command, spec.Args...)
	cmd.Env = append(os.Environ(), spec.Env...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, nil, err
	}
	conn := &processConn{r: stdout, w: stdin, cmd: cmd}
	return conn, conn, nil
}

// processConn aggregates the child process pipes and reaps the child on close.
type processConn struct {
	r   io.ReadCloser
	w   io.WriteCloser
	cmd *exec.Cmd

	closeOnce sync.Once
	closeErr  error
}

func (p *processConn) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *processConn) Write(b []byte) (int, error) { return p.w.Write(b) }

func (p *processConn) Close() error {
	p.closeOnce.Do(func() {
		_ = p.w.Close()
		done := make(chan error, 1)
		go func() { done <- p.cmd.Wait() }()
		select {
		case p.closeErr = <-done:
		case <-time.After(5 * time.Second):
			_ = p.cmd.Process.Kill()
			<-done
			p.closeErr = context.DeadlineExceeded
		}
	})
	return p.closeErr
}

// Client and callbacks are aliases so existing dispatch users can migrate to
// ai-acp incrementally without keeping a second protocol implementation.
type Client = aiacp.Client
type ClientCallbacks = aiacp.ClientCallbacks

// NewClient constructs the generic ACP client through the dispatch package's
// compatibility boundary.
func NewClient(name string, rwc io.ReadWriteCloser, closeFn func() error) *Client {
	return aiacp.NewClient(name, rwc, closeFn)
}

// StartWorker launches a worker and connects an ai-acp.Client to it.
func StartWorker(ctx context.Context, spec WorkerSpec) (*Client, error) {
	return StartWorkerWithOpener(ctx, spec, ProcessOpener)
}

// StartWorkerWithOpener allows tests to replace process setup with net.Pipe.
func StartWorkerWithOpener(ctx context.Context, spec WorkerSpec, opener Opener) (*Client, error) {
	rwc, closer, err := opener(ctx, spec)
	if err != nil {
		return nil, err
	}
	if closer != nil {
		rwc = &managedConn{ReadWriteCloser: rwc, closer: closer}
	}
	client := aiacp.NewClient(spec.Name, rwc, nil)
	return client, nil
}

type managedConn struct {
	io.ReadWriteCloser
	closer    io.Closer
	closeOnce sync.Once
	closeErr  error
}

func (c *managedConn) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.closer.Close() })
	return c.closeErr
}
