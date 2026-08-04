package memory

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
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
	conv := []Message{{Role: "user", Content: "q"}, {Role: "assistant", Content: "a"}}
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

	sum, err := ApplyCheckpoint(s, res, openTasks, nil, "cp-9", 123, "/work")
	if err != nil {
		t.Fatalf("ApplyCheckpoint: %v", err)
	}
	if sum.Count() != 3 { // 新建 + 关闭更新 + add fact
		t.Errorf("Count = %d, want 3", sum.Count())
	}
	// 明细：新建归入 UpdatedTasks、done 归入 ClosedTasks、fact 带 topic 前缀
	if !slices.Equal(sum.UpdatedTasks, []string{"fix config parser"}) {
		t.Errorf("UpdatedTasks = %v, want [fix config parser]", sum.UpdatedTasks)
	}
	if !slices.Equal(sum.ClosedTasks, []string{"old task"}) {
		t.Errorf("ClosedTasks = %v, want [old task]", sum.ClosedTasks)
	}
	if !slices.Equal(sum.Facts, []string{"config: project uses yaml"}) {
		t.Errorf("Facts = %v, want [config: project uses yaml]", sum.Facts)
	}
	if len(sum.DeletedFacts) != 0 {
		t.Errorf("DeletedFacts = %v, want empty", sum.DeletedFacts)
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
	sum, err := ApplyCheckpoint(s, res, nil, nil, "cp-1", 0, "")
	if err != nil {
		t.Fatalf("ApplyCheckpoint: %v", err)
	}
	if sum.Count() != 0 {
		t.Errorf("Count = %d, want 0", sum.Count())
	}
	tasks, _ := s.ListTasks(10)
	if len(tasks) != 0 {
		t.Fatalf("tasks = %d, want 0", len(tasks))
	}
}

func TestApplyCheckpoint_DeleteOnlyCounts(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	// 已有 fact，固化时模型判定删除 —— delete 也必须计入明细，否则误报"无新记忆"
	s.AddFact(&Fact{ID: "f_stale", Content: "outdated info", Topic: "t"})
	current, _ := s.ListFacts(10)

	res := &CheckpointResult{
		Decisions: []Decision{{Action: DecisionDelete, ID: "f_stale"}},
	}
	sum, err := ApplyCheckpoint(s, res, nil, current, "cp-del", 0, "")
	if err != nil {
		t.Fatalf("ApplyCheckpoint: %v", err)
	}
	if sum.Count() != 1 {
		t.Errorf("Count = %d, want 1 (delete must count)", sum.Count())
	}
	if !slices.Equal(sum.DeletedFacts, []string{"t: outdated info"}) {
		t.Errorf("DeletedFacts = %v, want [t: outdated info]", sum.DeletedFacts)
	}
	facts, _ := s.ListFacts(10)
	if len(facts) != 0 {
		t.Errorf("facts = %d, want 0 (deleted)", len(facts))
	}
}

func TestApplyCheckpoint_DuplicateTaskIDDedup(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	// 同 id 两条 TaskUpdate：只按最终 done 状态记一条明细，不重复渲染、不虚高计数
	s.UpsertTask(&Task{Title: "dup task", Status: "open"})
	openTasks, _ := s.OpenTasks(10)
	id := openTasks[0].ID

	res := &CheckpointResult{Tasks: []TaskUpdate{
		{ID: id, SummaryDelta: "phase 1"},
		{ID: id, SummaryDelta: "phase 2", Done: true},
	}}
	sum, err := ApplyCheckpoint(s, res, openTasks, nil, "cp-d", 0, "")
	if err != nil {
		t.Fatalf("ApplyCheckpoint: %v", err)
	}
	if len(sum.UpdatedTasks) != 0 {
		t.Errorf("UpdatedTasks = %v, want empty (final done)", sum.UpdatedTasks)
	}
	if !slices.Equal(sum.ClosedTasks, []string{"dup task"}) {
		t.Errorf("ClosedTasks = %v, want [dup task]", sum.ClosedTasks)
	}
	if sum.Count() != 1 {
		t.Errorf("Count = %d, want 1", sum.Count())
	}
}
