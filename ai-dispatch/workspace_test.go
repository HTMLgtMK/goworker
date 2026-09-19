package dispatch

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// initRepo 建一个带初始 commit 的真实 git 仓库。
func initRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	git("init", "-q")
	git("config", "user.email", "dispatch@test")
	git("config", "user.name", "dispatch")
	git("commit", "--allow-empty", "-q", "-m", "init")
	return dir
}

func TestDetectGit(t *testing.T) {
	ctx := context.Background()
	repo := initRepo(t)
	if !DetectGit(ctx, repo) {
		t.Error("repo should be detected as git work tree")
	}
	plain := t.TempDir()
	if DetectGit(ctx, plain) {
		t.Error("plain dir should not be detected as git work tree")
	}
}

func TestWorktreeLifecycleAndCommitCollection(t *testing.T) {
	ctx := context.Background()
	repo := initRepo(t)
	base, err := HeadCommit(ctx, repo)
	if err != nil {
		t.Fatalf("HeadCommit: %v", err)
	}

	wt := filepath.Join(repo, ".goworker", "dispatch", "task_x")
	if err := AddWorktree(ctx, repo, wt, "dispatch/task_x"); err != nil {
		t.Fatalf("AddWorktree: %v", err)
	}
	if !DetectGit(ctx, wt) {
		t.Fatal("worktree should be a git work tree")
	}

	// 在 worktree 里产生一个 commit
	commit := exec.Command("git", "-C", wt, "commit", "--allow-empty", "-m", "fake work")
	if out, err := commit.CombinedOutput(); err != nil {
		t.Fatalf("commit in worktree: %v: %s", err, out)
	}

	commits, err := CollectCommits(ctx, wt, base)
	if err != nil {
		t.Fatalf("CollectCommits: %v", err)
	}
	if len(commits) != 1 || !strings.Contains(commits[0], "fake work") {
		t.Errorf("commits = %v, want exactly the fake work commit", commits)
	}

	if err := RemoveWorktree(ctx, repo, wt, "dispatch/task_x", true); err != nil {
		t.Fatalf("RemoveWorktree: %v", err)
	}
	if _, err := exec.Command("git", "-C", repo, "rev-parse", "--verify", "dispatch/task_x").CombinedOutput(); err == nil {
		t.Error("branch should be deleted with worktree")
	}
}

// ---- runner 注入的纯逻辑测试（不依赖 git 二进制）----

func TestCollectCommits_EmptyDiff(t *testing.T) {
	fake := func(_ context.Context, _ string, args ...string) (string, error) {
		if strings.Join(args, " ") != "log --oneline abc..HEAD" {
			t.Errorf("unexpected args: %v", args)
		}
		return "", nil
	}
	commits, err := collectCommitsWith(context.Background(), fake, "/dir", "abc")
	if err != nil || len(commits) != 0 {
		t.Errorf("commits = %v err = %v, want empty", commits, err)
	}
}

func TestCollectCommits_TrimsAndSkipsBlanks(t *testing.T) {
	fake := func(context.Context, string, ...string) (string, error) {
		return "abc1 feat: one\n\n  abc2 fix: two  \n", nil
	}
	commits, err := collectCommitsWith(context.Background(), fake, "/dir", "base")
	if err != nil {
		t.Fatal(err)
	}
	if len(commits) != 2 || commits[0] != "abc1 feat: one" || commits[1] != "abc2 fix: two" {
		t.Errorf("commits = %#v", commits)
	}
}

func TestDetectGit_NegativeOnCommandError(t *testing.T) {
	fake := func(context.Context, string, ...string) (string, error) {
		return "fatal: not a git repository", errors.New("exit 128")
	}
	if detectGitWith(context.Background(), fake, "/dir") {
		t.Error("should not detect git on error")
	}
}
