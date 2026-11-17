package service

import (
	"context"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-runtime/session"
)

func TestVersionZeroSnapshotBecomesNonempty(t *testing.T) {
	repo, err := session.OpenRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	store, err := repo.Create(context.Background(), session.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p := &AgentPlugin{repo: repo}
	wireTitleControl(t, p, store)
	before, err := p.GetSessionTitle(store.SessionID())
	if err != nil || before.Title != "" || before.TitleVersion != 0 {
		t.Fatalf("before = %+v, %v", before, err)
	}
	if _, err := store.Commit([]core.Message{{MsgID: "first", Role: "user", Content: "first question"}}); err != nil {
		t.Fatal(err)
	}
	after, err := p.GetSessionTitle(store.SessionID())
	if err != nil || after.Title != "first question" || after.TitleVersion != 0 {
		t.Fatalf("after = %+v, %v", after, err)
	}
}
