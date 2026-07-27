package stdin

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/tinguo/goworker/daemon/internal/core"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

type StdinFrontend struct {
	engine *core.Engine
	mu     sync.Mutex
	reader *bufio.Reader
}

func NewStdinFrontend(engine *core.Engine) *StdinFrontend {
	return &StdinFrontend{engine: engine}
}

func (frontend *StdinFrontend) Write(s string) {
	fmt.Println(s)
}

// readLine 供 agent 的 HITL 确认使用。
// 与主循环共享同一个 bufio.Reader，不再有 scanner/reader 竞争 stdin 的问题。
func (frontend *StdinFrontend) readLine() (string, error) {
	frontend.mu.Lock()
	defer frontend.mu.Unlock()
	return frontend.reader.ReadString('\n')
}

func (frontend *StdinFrontend) Run() error {
	frontend.reader = bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")

		frontend.mu.Lock()
		line, err := frontend.reader.ReadString('\n')
		frontend.mu.Unlock()

		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("read stdin: %w", err)
		}

		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			continue
		case line == "/quit" || line == "/exit" || line == "/q":
			fmt.Println("bye")
			return nil
		}

		ctx := spec.NewContext(frontend.Write, frontend.readLine, nil)
		if err := frontend.engine.Eval(ctx, line); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}
}
