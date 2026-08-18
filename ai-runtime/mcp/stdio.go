package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
)

// StdioTransport 通过子进程 stdin/stdout 承载 JSON-RPC 2.0。
//
// MCP stdio transport 的消息是 newline-delimited JSON（每行一条消息），
// 不像 LSP 用 Content-Length 头。响应靠 JSON-RPC 的 id 与请求配对。
type StdioTransport struct {
	cmd     *exec.Cmd
	writer  *bufio.Writer
	mu      sync.Mutex // 串行化写入 + 保护 pending/nextID
	pending map[int]chan rpcMessage
	nextID  int

	closeOnce sync.Once
}

// NewStdioTransport 启动 command 进程并建立 stdio 通道。
func NewStdioTransport(command string, args []string) (*StdioTransport, error) {
	cmd := exec.Command(command, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	// server 的日志走 stderr 透传，调试 MCP 服务器时别把它闷死
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", command, err)
	}

	t := &StdioTransport{
		cmd:     cmd,
		writer:  bufio.NewWriter(stdin),
		pending: make(map[int]chan rpcMessage),
	}
	go t.readLoop(stdout)
	return t, nil
}

// Call 发送一个请求并等待对应 id 的响应（受 ctx 取消约束）。
// result 为 nil 时丢弃响应体；否则解码进 result。
func (t *StdioTransport) Call(ctx context.Context, method string, params, result any) error {
	t.mu.Lock()
	t.nextID++
	id := t.nextID
	body, err := encodeRequest(id, method, params)
	if err != nil {
		t.mu.Unlock()
		return err
	}
	ch := make(chan rpcMessage, 1)
	t.pending[id] = ch
	if err := t.writeLocked(body); err != nil {
		delete(t.pending, id)
		t.mu.Unlock()
		return err
	}
	t.mu.Unlock()

	select {
	case msg, ok := <-ch:
		if !ok {
			return fmt.Errorf("mcp transport closed while waiting for response")
		}
		if msg.Error != nil {
			return msg.Error
		}
		if result == nil || len(msg.Result) == 0 {
			return nil
		}
		return json.Unmarshal(msg.Result, result)
	case <-ctx.Done():
		// 清理 pending，避免挂起调用在 transport 活着的会话里越积越多
		t.mu.Lock()
		delete(t.pending, id)
		t.mu.Unlock()
		return ctx.Err()
	}
}

// Notify 发送一个无响应的 notification（fire-and-forget，如 initialized）。
func (t *StdioTransport) Notify(ctx context.Context, method string, params any) error {
	body, err := encodeNotification(method, params)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writeLocked(body)
}

// writeLocked 必须持 t.mu 调用。
func (t *StdioTransport) writeLocked(body []byte) error {
	if _, err := t.writer.Write(body); err != nil {
		return fmt.Errorf("write rpc: %w", err)
	}
	if err := t.writer.WriteByte('\n'); err != nil {
		return fmt.Errorf("write rpc newline: %w", err)
	}
	return t.writer.Flush()
}

// readLoop 消费 stdout，把响应按 id 投递给等待的 Call。
// stdout 关闭（进程退出）或扫描出错（单行超限）时唤醒所有挂起调用。
func (t *StdioTransport) readLoop(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	// 工具结果可能很大（递归 ls、整文件内容），默认 64KB 会截断
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var msg rpcMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			continue // 垃圾行忽略；真正的协议错误会表现为等待超时
		}
		if len(msg.ID) == 0 {
			// server 主动通知（无 id），当前阶段忽略
			continue
		}
		var id int
		if err := json.Unmarshal(msg.ID, &id); err != nil {
			continue
		}
		t.mu.Lock()
		ch, ok := t.pending[id]
		if ok {
			delete(t.pending, id)
		}
		t.mu.Unlock()
		if ok {
			ch <- msg
		}
	}

	// 区分正常 EOF（进程退出）和扫描错误（单行超 16MB buffer）：
	// 后者意味着 pipe 里可能还有数据，连接已不可信，同样按关闭处理
	if err := scanner.Err(); err != nil {
		slog.Error("mcp stdio scanner error, transport unusable", "err", err)
	}

	// stdout 关闭：唤醒所有挂起调用，让它们以 transport closed 失败
	t.mu.Lock()
	for id, ch := range t.pending {
		delete(t.pending, id)
		close(ch)
	}
	t.mu.Unlock()
}

// Close 终止进程并回收资源。
// Kill 失败时用 Wait 确认进程是否已自行退出；两者都失败才返回错误。
func (t *StdioTransport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		if t.cmd.Process != nil {
			if kerr := t.cmd.Process.Kill(); kerr != nil {
				err = t.cmd.Wait()
				return
			}
		}
		// 主动 kill 后回收僵尸，非 0 退出码是预期的，不算错误
		t.cmd.Wait()
	})
	return err
}
