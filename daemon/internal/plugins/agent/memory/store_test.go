package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func newTestStore(t *testing.T, taskKeep int) *FileStore {
	t.Helper()
	s, err := NewFileStore(t.TempDir(), taskKeep)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestFileStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir, 0)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := s.AddFact(&Fact{Content: "build with go build ./...", Topic: "tooling", Source: "user"}); err != nil {
		t.Fatalf("AddFact: %v", err)
	}
	if err := s.UpsertTask(&Task{Title: "fix config", Status: "open", Summary: "started"}); err != nil {
		t.Fatalf("UpsertTask: %v", err)
	}
	s.Close()

	s2, err := NewFileStore(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	facts, _ := s2.ListFacts(10)
	if len(facts) != 1 || facts[0].Content != "build with go build ./..." {
		t.Fatalf("facts after reopen = %+v, want 1 persisted fact", facts)
	}
	tasks, _ := s2.ListTasks(10)
	if len(tasks) != 1 || tasks[0].Title != "fix config" {
		t.Fatalf("tasks after reopen = %+v, want 1 persisted task", tasks)
	}
}

func TestFileStore_AddSkipsExactDuplicate(t *testing.T) {
	s := newTestStore(t, 0)
	s.AddFact(&Fact{Content: "dup fact", Source: "user"})
	s.AddFact(&Fact{Content: "dup fact", Source: "user"}) // 大小写/空白不同也视为重复
	s.AddFact(&Fact{Content: "DUP fact ", Source: "user"})
	facts, _ := s.ListFacts(10)
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want 1 (duplicates skipped)", len(facts))
	}
}

func TestFileStore_UpdateKeepsIDAndCreatedAt(t *testing.T) {
	s := newTestStore(t, 0)
	f := &Fact{Content: "old", Topic: "t", Source: "checkpoint:cp-1"}
	if err := s.AddFact(f); err != nil {
		t.Fatalf("AddFact: %v", err)
	}
	id, created := f.ID, f.CreatedAt

	if err := s.UpdateFact(&Fact{ID: id, Content: "new", Topic: "t2"}); err != nil {
		t.Fatalf("UpdateFact: %v", err)
	}
	facts, _ := s.ListFacts(10)
	if len(facts) != 1 || facts[0].Content != "new" || facts[0].Topic != "t2" {
		t.Fatalf("after update = %+v", facts)
	}
	if facts[0].ID != id {
		t.Errorf("ID changed: %s → %s", id, facts[0].ID)
	}
	if !facts[0].CreatedAt.Equal(created) {
		t.Errorf("CreatedAt changed: %v → %v", created, facts[0].CreatedAt)
	}
}

func TestFileStore_UpdateUnknownIDIsNoop(t *testing.T) {
	s := newTestStore(t, 0)
	if err := s.UpdateFact(&Fact{ID: "nope", Content: "x"}); err != nil {
		t.Fatalf("UpdateFact unknown id: %v", err)
	}
}

func TestFileStore_DeleteIsIdempotent(t *testing.T) {
	s := newTestStore(t, 0)
	f := &Fact{Content: "to delete"}
	s.AddFact(f)
	if err := s.DeleteFact(f.ID); err != nil {
		t.Fatalf("DeleteFact: %v", err)
	}
	if err := s.DeleteFact(f.ID); err != nil { // 删不存在的 id 不报错
		t.Fatalf("DeleteFact again: %v", err)
	}
	facts, _ := s.ListFacts(10)
	if len(facts) != 0 {
		t.Fatalf("facts = %d, want 0", len(facts))
	}
}

func TestFileStore_SearchFiltersAndRanks(t *testing.T) {
	s := newTestStore(t, 0)
	s.AddFact(&Fact{Content: "deploy uses make deploy", Topic: "deploy"})
	s.AddFact(&Fact{Content: "database is postgres", Topic: "db"})
	s.AddFact(&Fact{Content: "ci runs golangci-lint", Topic: "ci"})

	got, _ := s.SearchFacts("postgres", 10)
	if len(got) != 1 || got[0].Topic != "db" {
		t.Fatalf("search 'postgres' = %+v, want the db fact", got)
	}
	got, _ = s.SearchFacts("kubernetes", 10)
	if len(got) != 0 {
		t.Fatalf("search 'kubernetes' = %+v, want none", got)
	}
}

func TestFileStore_UpsertTask_NewAndUpdate(t *testing.T) {
	s := newTestStore(t, 0)
	task := &Task{Title: "fix config", Status: "open", Summary: "started", Runs: []string{"run-1"}}
	if err := s.UpsertTask(task); err != nil {
		t.Fatalf("UpsertTask: %v", err)
	}
	id := task.ID
	if id == "" {
		t.Fatal("new task should get an id")
	}
	if task.Status != "open" {
		t.Errorf("Status = %q, want open", task.Status)
	}
	created := task.CreatedAt

	// 更新：追加 run、替换摘要
	if err := s.UpsertTask(&Task{ID: id, Title: "fix config", Summary: "more progress", Runs: []string{"run-2"}}); err != nil {
		t.Fatalf("UpsertTask update: %v", err)
	}
	all, _ := s.ListTasks(10)
	if len(all) != 1 {
		t.Fatalf("tasks = %d, want 1", len(all))
	}
	if all[0].Summary != "more progress" {
		t.Errorf("Summary = %q, want updated", all[0].Summary)
	}
	if len(all[0].Runs) != 2 || all[0].Runs[1] != "run-2" {
		t.Errorf("Runs = %v, want [run-1 run-2]", all[0].Runs)
	}
	if !all[0].CreatedAt.Equal(created) {
		t.Errorf("CreatedAt changed: %v → %v", created, all[0].CreatedAt)
	}
}

func TestFileStore_UpsertTask_UnknownIDIgnored(t *testing.T) {
	s := newTestStore(t, 0)
	if err := s.UpsertTask(&Task{ID: "ghost", Title: "x"}); err != nil {
		t.Fatalf("UpsertTask unknown id: %v", err)
	}
	tasks, _ := s.ListTasks(10)
	if len(tasks) != 0 {
		t.Fatalf("tasks = %d, want 0", len(tasks))
	}
}

func TestFileStore_OpenTasksFiltersClosed(t *testing.T) {
	s := newTestStore(t, 0)
	for _, title := range []string{"a", "b", "c"} {
		s.UpsertTask(&Task{Title: title, Status: "open"})
	}
	s.CloseTask(s.mustTaskID(t, "b"))

	open, _ := s.OpenTasks(10)
	if len(open) != 2 {
		t.Fatalf("open tasks = %d, want 2", len(open))
	}
	all, _ := s.ListTasks(10)
	if len(all) != 3 {
		t.Fatalf("all tasks = %d, want 3 (closed kept)", len(all))
	}
}

func (s *FileStore) mustTaskID(t *testing.T, title string) string {
	t.Helper()
	tasks, _ := s.ListTasks(10)
	for _, tk := range tasks {
		if tk.Title == title {
			return tk.ID
		}
	}
	t.Fatalf("task %q not found", title)
	return ""
}

func TestFileStore_CloseTaskIdempotent(t *testing.T) {
	s := newTestStore(t, 0)
	s.UpsertTask(&Task{Title: "x", Status: "open"})
	id := s.mustTaskID(t, "x")

	if err := s.CloseTask(id); err != nil {
		t.Fatalf("CloseTask: %v", err)
	}
	if err := s.CloseTask(id); err != nil { // 重复关闭不报错
		t.Fatalf("CloseTask again: %v", err)
	}
	if err := s.CloseTask("ghost"); err != nil { // 未知 id 不报错
		t.Fatalf("CloseTask ghost: %v", err)
	}
	tasks, _ := s.ListTasks(10)
	if tasks[0].Status != "closed" {
		t.Errorf("Status = %q, want closed", tasks[0].Status)
	}
}

func TestFileStore_TaskKeepTrim(t *testing.T) {
	s := newTestStore(t, 2) // 只保留最近 2 条
	for i := 0; i < 4; i++ {
		s.UpsertTask(&Task{Title: fmt.Sprintf("task-%d", i), Status: "open", Summary: fmt.Sprintf("s%d", i)})
	}
	all, _ := s.ListTasks(10)
	if len(all) != 2 {
		t.Fatalf("tasks = %d, want 2 after trim", len(all))
	}
	if all[0].Title != "task-3" || all[1].Title != "task-2" {
		t.Errorf("after trim order = [%s, %s], want [task-3, task-2]", all[0].Title, all[1].Title)
	}
}

func TestFileStore_SkipsCorruptLine(t *testing.T) {
	dir := t.TempDir()
	ltmPath := filepath.Join(dir, "ltm", "facts.jsonl")
	if err := os.MkdirAll(filepath.Dir(ltmPath), 0o700); err != nil {
		t.Fatal(err)
	}
	good := `{"id":"f_1","content":"good","source":"user"}`
	// 坏行：截断的 JSON
	if err := os.WriteFile(ltmPath, []byte(good+"\n{this is not json\n"+good+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := NewFileStore(dir, 0)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	defer s.Close()
	facts, _ := s.ListFacts(10)
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want 2 (corrupt line skipped)", len(facts))
	}
}

func TestFileStore_ConcurrentAdds(t *testing.T) {
	s := newTestStore(t, 0)
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.AddFact(&Fact{Content: fmt.Sprintf("fact %d", i), Source: "user"})
		}(i)
	}
	wg.Wait()
	facts, _ := s.ListFacts(100)
	if len(facts) != n {
		t.Fatalf("facts = %d, want %d", len(facts), n)
	}
}
