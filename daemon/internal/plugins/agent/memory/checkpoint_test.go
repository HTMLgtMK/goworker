package memory

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

func TestParseCheckpoint(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantT   int
		wantD   int
		wantErr bool
	}{
		{name: "plain json", in: `{"tasks":[{"id":"t_1","summary_delta":"x"}],"decisions":[]}`, wantT: 1, wantD: 0},
		{name: "code fence", in: "```json\n{\"tasks\":[{\"title\":\"new\",\"summary_delta\":\"y\"}],\"decisions\":[{\"action\":\"add\",\"content\":\"z\"}]}\n```", wantT: 1, wantD: 1},
		{name: "noise around", in: "ok here: {\"tasks\":[{\"id\":\"t_1\"}],\"decisions\":[]} done", wantT: 1, wantD: 0},
		{name: "empty", in: `{"tasks":[],"decisions":[]}`, wantT: 0, wantD: 0},
		{name: "no title no id dropped", in: `{"tasks":[{"summary_delta":"x"},{"id":"t_1"}],"decisions":[]}`, wantT: 1},
		{name: "bad decision action dropped", in: `{"tasks":[],"decisions":[{"action":"explode"},{"action":"add","content":"ok"}]}`, wantD: 1},
		{name: "no json", in: "the model said no", wantErr: true},
		{name: "invalid json", in: `{"tasks": [}`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCheckpoint(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCheckpoint: %v", err)
			}
			if len(got.Tasks) != tc.wantT {
				t.Errorf("tasks = %d, want %d (%+v)", len(got.Tasks), tc.wantT, got.Tasks)
			}
			if len(got.Decisions) != tc.wantD {
				t.Errorf("decisions = %d, want %d", len(got.Decisions), tc.wantD)
			}
		})
	}
}

func TestCheckpointer_PrependsSystemPromptAndConversation(t *testing.T) {
	stub := &stubProvider{resp: `{"tasks":[],"decisions":[]}`}
	cp := NewCheckpointer(stub, "/work")
	conv := []core.Message{{Role: "user", Content: "q"}, {Role: "assistant", Content: "a"}}
	openTasks := []Task{{ID: "t_9", Title: "existing", Summary: "s"}}
	facts := []Fact{{ID: "f_9", Content: "fact"}}

	if _, err := cp.Run(context.Background(), conv, openTasks, facts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	req := stub.lastReq
	if len(req.Messages) != len(conv)+1 {
		t.Fatalf("messages = %d, want %d (system prompt + conversation)", len(req.Messages), len(conv)+1)
	}
	if req.Messages[0].Role != "system" {
		t.Errorf("Messages[0].Role = %q, want system", req.Messages[0].Role)
	}
	for i, m := range conv {
		if got := req.Messages[i+1]; got.Role != m.Role || got.Content != m.Content {
			t.Errorf("Messages[%d] = %+v, want %+v", i+1, got, m)
		}
	}
	// prompt 带上 open task id 与 fact id，供模型引用
	if !strings.Contains(req.Messages[0].Content, "t_9") || !strings.Contains(req.Messages[0].Content, "f_9") {
		t.Errorf("prompt should list open task ids and fact ids")
	}
}

func TestCheckpointer_ProviderError(t *testing.T) {
	stub := &stubProvider{err: errors.New("boom")}
	if _, err := NewCheckpointer(stub, "").Run(context.Background(), nil, nil, nil); err == nil {
		t.Fatal("want error from provider")
	}
}

func TestApplyCheckpoint_NewTaskAndClose(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	// 先建一个 open task（走正常路径拿真实 ID），再固化时模型引用它
	s.UpsertTask(&Task{Title: "old task", Status: "open", Summary: "prior"})
	openTasks, _ := s.OpenTasks(10)
	oldID := openTasks[0].ID

	res := &CheckpointResult{
		Tasks: []TaskUpdate{
			{Title: "fix config parser", SummaryDelta: "found the bug", NextSteps: []string{"write test"}},
			{ID: oldID, SummaryDelta: "wrapped up", Done: true},
		},
		Decisions: []Decision{{Action: DecisionAdd, Content: "project uses yaml", Topic: "config"}},
	}

	applied, err := ApplyCheckpoint(s, res, openTasks, nil, "cp-9", 123, "/work")
	if err != nil {
		t.Fatalf("ApplyCheckpoint: %v", err)
	}
	if applied != 3 { // 新建 + 关闭更新 + add fact
		t.Errorf("applied = %d, want 3", applied)
	}

	all, _ := s.ListTasks(10)
	if len(all) != 2 {
		t.Fatalf("tasks = %d, want 2 (%+v)", len(all), all)
	}
	// 新建 task：open + summary + cwd + run
	var created *Task
	for i := range all {
		if all[i].ID == oldID {
			if all[i].Status != "closed" {
				t.Errorf("t_old Status = %q, want closed", all[i].Status)
			}
			if all[i].Summary != "prior\nwrapped up" {
				t.Errorf("t_old Summary = %q, want merged", all[i].Summary)
			}
		} else {
			created = &all[i]
		}
	}
	if created == nil || created.Status != "open" || created.Title != "fix config parser" {
		t.Fatalf("new task wrong: %+v", all)
	}
	if created.Cwd != "/work" || len(created.Runs) != 1 || created.Runs[0] != "cp-9" || created.TokenUsage != 123 {
		t.Errorf("new task meta wrong: %+v", created)
	}

	facts, _ := s.ListFacts(10)
	if len(facts) != 1 || facts[0].Source != "checkpoint:cp-9" {
		t.Errorf("fact not added with source: %+v", facts)
	}
}

func TestApplyCheckpoint_ReferencesUnknownTaskIgnored(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	res := &CheckpointResult{Tasks: []TaskUpdate{{ID: "ghost", SummaryDelta: "x"}}}
	applied, err := ApplyCheckpoint(s, res, nil, nil, "cp-1", 0, "")
	if err != nil {
		t.Fatalf("ApplyCheckpoint: %v", err)
	}
	if applied != 0 {
		t.Errorf("applied = %d, want 0", applied)
	}
	tasks, _ := s.ListTasks(10)
	if len(tasks) != 0 {
		t.Fatalf("tasks = %d, want 0", len(tasks))
	}
}
