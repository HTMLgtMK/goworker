package task

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTask(prompt string) *Task {
	now := time.Now()
	return &Task{
		ID:        NewID(),
		Source:    "repl",
		Kind:      KindCode,
		Prompt:    prompt,
		Repo:      "/repo",
		Worker:    "claude",
		Status:    StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func TestTask_TransitionHappyPath(t *testing.T) {
	tk := newTask("fix the bug")
	path := []Status{StatusDispatching, StatusWorking, StatusAwaitingReview, StatusMerging, StatusDone}
	for _, to := range path {
		if err := tk.Transition(to); err != nil {
			t.Fatalf("transition %s -> %s: %v", tk.Status, to, err)
		}
	}
	if !tk.Status.Terminal() {
		t.Errorf("final status %q should be terminal", tk.Status)
	}
}

func TestTask_RejectPath(t *testing.T) {
	tk := newTask("x")
	if err := tk.Transition(StatusWorking); err == nil {
		t.Fatal("queued -> working should be invalid")
	}
	if !errors.Is(tk.Transition(StatusWorking), ErrInvalidTransition) {
		t.Fatal("want ErrInvalidTransition")
	}
}

func TestTask_TerminalIsFrozen(t *testing.T) {
	tk := newTask("x")
	if err := tk.FailInto("boom"); err != nil {
		t.Fatalf("FailInto: %v", err)
	}
	if err := tk.Transition(StatusQueued); err == nil {
		t.Fatal("failed task must not transition")
	}
	if tk.Error != "boom" {
		t.Errorf("error = %q", tk.Error)
	}
}

func TestStore_AddUpdateList(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "dispatch", "tasks.jsonl"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	a := newTask("task a")
	if err := store.Add(a); err != nil {
		t.Fatalf("Add: %v", err)
	}
	b := newTask("task b")
	if err := store.Add(b); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if err := a.Transition(StatusDispatching); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(a); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, ok := store.Get(a.ID)
	if !ok || got.Status != StatusDispatching {
		t.Fatalf("Get = %+v ok=%v", got, ok)
	}
	list := store.List()
	if len(list) != 2 || list[0].ID != a.ID || list[1].ID != b.ID {
		t.Errorf("List order = %v", list)
	}
}

func TestStore_ReplayAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.jsonl")

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a := newTask("survive restart")
	if err := store.Add(a); err != nil {
		t.Fatal(err)
	}
	if err := a.Transition(StatusDispatching); err != nil {
		t.Fatal(err)
	}
	a.Worktree = "/repo/.goworker/dispatch/x"
	if err := store.Update(a); err != nil {
		t.Fatal(err)
	}
	store.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, ok := reopened.Get(a.ID)
	if !ok {
		t.Fatal("task lost after reopen")
	}
	if got.Status != StatusDispatching || got.Worktree != a.Worktree {
		t.Errorf("replayed = %+v", got)
	}
}

func TestStore_ReplaySkipsCorruptLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.jsonl")

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a := newTask("good one")
	if err := store.Add(a); err != nil {
		t.Fatal(err)
	}
	store.Close()

	// 模拟崩溃残留：追加半行 JSON
	f, err := osOpenAppend(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"id":"task_half","status":"que`)
	_ = f.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen with corrupt tail: %v", err)
	}
	defer reopened.Close()
	list := reopened.List()
	if len(list) != 1 || list[0].ID != a.ID {
		t.Errorf("list = %+v, want only the good task", list)
	}
}

func TestStore_DuplicateAndUnknown(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "tasks.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	a := newTask("dup")
	if err := store.Add(a); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(a); err == nil {
		t.Fatal("duplicate id should fail")
	}

	ghost := newTask("ghost")
	ghost.ID = "task_missing"
	if err := store.Update(ghost); err == nil {
		t.Fatal("update unknown id should fail")
	}
}

func osOpenAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
}
