package dispatch

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
)

// listingHandler 实现 TaskHandler + SessionLoader + SessionLister：具备 load/list 能力。
type listingHandler struct{}

func (listingHandler) Run(_ context.Context, _, _ string, _ Reporter) (string, error) {
	return protocol.StopEndTurn, nil
}

func (listingHandler) LoadSession(sessionID string, _ Reporter) error {
	if sessionID != "sess_current" {
		return &protocol.RPCError{Code: -32000, Message: "dispatch: no such session " + sessionID}
	}
	return nil
}

func (listingHandler) ListSessions() []protocol.SessionInfo {
	return []protocol.SessionInfo{{SessionID: "sess_current", Title: "current work"}}
}

// emptyListHandler 能力同 listingHandler，但清单为空（验证空清单序列化为 []）。
type emptyListHandler struct{ listingHandler }

func (emptyListHandler) ListSessions() []protocol.SessionInfo { return nil }

// plainHandler 只实现 TaskHandler：无 load/list 能力（能力协商应为 false）。
type plainHandler struct{}

func (plainHandler) Run(_ context.Context, _, _ string, _ Reporter) (string, error) {
	return protocol.StopEndTurn, nil
}

// serveWith 在 net.Pipe 上启动 Server，返回已开始 Serve 的客户端侧 Conn。
func serveWith(t *testing.T, handler TaskHandler) *protocol.Conn {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, handler)
	t.Cleanup(func() { _ = server.Close() })
	conn := protocol.NewConn(clientEnd)
	t.Cleanup(func() { _ = conn.Close() })
	go func() { _ = conn.Serve() }()
	return conn
}

func initialize(t *testing.T, conn *protocol.Conn) protocol.InitializeResponse {
	t.Helper()
	var resp protocol.InitializeResponse
	err := conn.Call(context.Background(), protocol.MethodInitialize,
		protocol.InitializeRequest{ProtocolVersion: protocol.Version}, &resp)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return resp
}

func TestServer_AdvertisesLoadSessionOnlyWhenHandlerImplementsLoader(t *testing.T) {
	if resp := initialize(t, serveWith(t, listingHandler{})); !resp.AgentCapabilities.LoadSession {
		t.Errorf("LoadSession capability = false with SessionLoader handler, want true")
	}
	if resp := initialize(t, serveWith(t, plainHandler{})); resp.AgentCapabilities.LoadSession {
		t.Errorf("LoadSession capability = true without SessionLoader handler, want false")
	}
}

func TestServer_SessionListDelegatesToHandler(t *testing.T) {
	conn := serveWith(t, listingHandler{})
	initialize(t, conn)

	var resp protocol.ListSessionsResponse
	err := conn.Call(context.Background(), protocol.MethodSessionList,
		protocol.ListSessionsRequest{Cwd: "/repo"}, &resp)
	if err != nil {
		t.Fatalf("session/list: %v", err)
	}
	want := []protocol.SessionInfo{{SessionID: "sess_current", Cwd: "", Title: "current work", UpdatedAt: ""}}
	if len(resp.Sessions) != len(want) || resp.Sessions[0] != want[0] {
		t.Errorf("sessions = %+v, want %+v", resp.Sessions, want)
	}
	if resp.NextCursor != "" {
		t.Errorf("nextCursor = %q, want empty (清单一次给全，不翻页)", resp.NextCursor)
	}
}

func TestServer_SessionListEmptyMarshalsAsJSONArray(t *testing.T) {
	conn := serveWith(t, emptyListHandler{})
	initialize(t, conn)

	var raw json.RawMessage
	if err := conn.Call(context.Background(), protocol.MethodSessionList,
		protocol.ListSessionsRequest{}, &raw); err != nil {
		t.Fatalf("session/list: %v", err)
	}
	var probe struct {
		Sessions json.RawMessage `json:"sessions"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		t.Fatalf("unmarshal result %s: %v", raw, err)
	}
	if strings.TrimSpace(string(probe.Sessions)) != "[]" {
		t.Errorf("sessions = %s, want [] (空清单不得序列化为 null)", probe.Sessions)
	}
}

func TestServer_SessionListRejectedWithoutLister(t *testing.T) {
	conn := serveWith(t, plainHandler{})
	initialize(t, conn)

	err := conn.Call(context.Background(), protocol.MethodSessionList,
		protocol.ListSessionsRequest{}, &protocol.ListSessionsResponse{})
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, "not supported") {
		t.Errorf("session/list error = %v, want RPCError(-32000, not supported)", err)
	}
}

func TestServer_SessionLoadDelegatesToHandler(t *testing.T) {
	conn := serveWith(t, listingHandler{})
	initialize(t, conn)

	// 成功路径：result 按 ACP 序列化为 {}（modes 可省）
	req := protocol.LoadSessionRequest{SessionID: "sess_current", Cwd: "/repo", McpServers: []map[string]any{}}
	var raw json.RawMessage
	if err := conn.Call(context.Background(), protocol.MethodSessionLoad, req, &raw); err != nil {
		t.Fatalf("session/load: %v", err)
	}
	if strings.TrimSpace(string(raw)) != "{}" {
		t.Errorf("session/load result = %s, want {}", raw)
	}

	// handler 拒绝的错误原样透传给客户端
	req.SessionID = "sess_missing"
	err := conn.Call(context.Background(), protocol.MethodSessionLoad, req, &raw)
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || !strings.Contains(rpcErr.Message, "no such session") {
		t.Errorf("session/load error = %v, want handler RPCError mentioning no such session", err)
	}
}

func TestServer_SessionLoadValidatesAndRequiresCapability(t *testing.T) {
	conn := serveWith(t, listingHandler{})
	initialize(t, conn)
	err := conn.Call(context.Background(), protocol.MethodSessionLoad,
		protocol.LoadSessionRequest{Cwd: "/repo"}, &protocol.LoadSessionResponse{})
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || !strings.Contains(rpcErr.Message, "requires sessionId") {
		t.Errorf("session/load without sessionId error = %v, want requires sessionId", err)
	}

	unsupported := serveWith(t, plainHandler{})
	initialize(t, unsupported)
	err = unsupported.Call(context.Background(), protocol.MethodSessionLoad,
		protocol.LoadSessionRequest{SessionID: "sess_current"}, &protocol.LoadSessionResponse{})
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, "not supported") {
		t.Errorf("session/load without loader error = %v, want RPCError(-32000, not supported)", err)
	}
}

// replayHandler 在 load 处理期间经 rep 推两条历史通知（user/agent 各一）。
type replayHandler struct{}

func (replayHandler) Run(_ context.Context, _, _ string, _ Reporter) (string, error) {
	return protocol.StopEndTurn, nil
}

func (replayHandler) LoadSession(sessionID string, rep Reporter) error {
	rep.Update(sessionID, protocol.SessionUpdateBody{
		SessionUpdate: protocol.UpdateUserMessageChunk,
		Content:       &protocol.ContentBlock{Type: "text", Text: "历史问题"},
	})
	rep.MessageChunk(sessionID, "历史回答")
	return nil
}

// TestServer_SessionLoadReplaysNotificationsBeforeResponse 验证 load 处理期间
// handler 经 rep 推送的通知在 wire 上先于 load 响应写出。不走 protocol.Conn 读：
// 其 notification 回调经 go 异步派发，回调到达顺序无保证；net.Pipe 同步传递且
// 每个通知/响应各占一次底层 Write，直接顺序读 raw 行才是 wire 真实顺序。
func TestServer_SessionLoadReplaysNotificationsBeforeResponse(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	server := ServeConn(serverEnd, replayHandler{})
	t.Cleanup(func() { _ = server.Close() })
	t.Cleanup(func() { _ = clientEnd.Close() })
	_ = clientEnd.SetDeadline(time.Now().Add(5 * time.Second))

	req, err := json.Marshal(protocol.LoadSessionRequest{SessionID: "sess_current", Cwd: "/repo"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	line := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`+"\n", protocol.MethodSessionLoad, req)
	if _, err := clientEnd.Write([]byte(line)); err != nil {
		t.Fatalf("write session/load request: %v", err)
	}

	reader := bufio.NewReader(clientEnd)
	type frame struct {
		Method string          `json:"method"`
		ID     json.RawMessage `json:"id"`
		Params struct {
			Update protocol.SessionUpdateBody `json:"update"`
		} `json:"params"`
		Error *protocol.RPCError `json:"error"`
	}
	readFrame := func() frame {
		t.Helper()
		raw, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatalf("read wire frame: %v", err)
		}
		var f frame
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("unmarshal wire frame %s: %v", raw, err)
		}
		return f
	}

	first := readFrame()
	if first.Method != protocol.MethodSessionUpdate || first.Params.Update.SessionUpdate != protocol.UpdateUserMessageChunk {
		t.Errorf("first frame = %+v, want session/update user_message_chunk", first)
	}
	second := readFrame()
	if second.Method != protocol.MethodSessionUpdate || second.Params.Update.SessionUpdate != protocol.UpdateAgentMessageChunk {
		t.Errorf("second frame = %+v, want session/update agent_message_chunk", second)
	}
	response := readFrame()
	if string(response.ID) != "1" || response.Error != nil {
		t.Errorf("load response = %+v, want success response for id 1", response)
	}
}
