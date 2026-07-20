package stdin

import (
	"bufio"
	"fmt"
	"os"

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
	stdinCh := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			stdinCh <- scanner.Text()
		}
		close(stdinCh)
	}()

	fmt.Print("> ")
	for line := range stdinCh {
		if line == "/quit" || line == "/exit" || line == "/q" {
			fmt.Println("bye")
			return nil
		}
		ctx := spec.NewContext(frontend.Write, nil)
		if err := frontend.engine.Eval(ctx, line); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
		fmt.Print("\n> ")
	}
	return nil
}
