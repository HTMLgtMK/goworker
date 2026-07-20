package stdin

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

type StdinFrontend struct {
	engine *core.Engine
}

func NewStdinFrontend(engine *core.Engine) *StdinFrontend {
	return &StdinFrontend{engine: engine}
}

func (frontend *StdinFrontend) Write(s string) {
	fmt.Println(s)
}

func (frontend *StdinFrontend) Run() error {
	// 信号监听
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// stdin goroutine
	stdinCh := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			stdinCh <- scanner.Text()
		}
		close(stdinCh)
	}()

	fmt.Print("> ")
	for {
		select {
		case line, ok := <-stdinCh:
			if !ok {
				return nil // stdin 关闭
			}
			if line == "/quit" || line == "/exit" || line == "/q" {
				fmt.Println("bye")
				return nil
			}
			ctx := spec.NewContext(frontend.Write, nil)
			if err := frontend.engine.Eval(ctx, line); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
			}
			fmt.Print("\n> ")

		case <-sigCh:
			fmt.Println("\nshutting down...")
			frontend.engine.StopAll()
			return nil
		}
	}
}
