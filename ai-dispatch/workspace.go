package dispatch

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// gitRunner 抽象 git 命令执行，测试可注入假实现。
type gitRunner func(ctx context.Context, dir string, args ...string) (string, error)

func runGit(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// DetectGit 报告 dir 是否在 git 工作树内。
func DetectGit(ctx context.Context, dir string) bool {
	return detectGitWith(ctx, runGit, dir)
}

func detectGitWith(ctx context.Context, run gitRunner, dir string) bool {
	out, err := run(ctx, dir, "rev-parse", "--is-inside-work-tree")
	return err == nil && strings.TrimSpace(out) == "true"
}

// HeadCommit 返回 dir 的 HEAD SHA。
func HeadCommit(ctx context.Context, dir string) (string, error) {
	return headCommitWith(ctx, runGit, dir)
}

func headCommitWith(ctx context.Context, run gitRunner, dir string) (string, error) {
	out, err := run(ctx, dir, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// AddWorktree 在 repo 上建隔离工作区：path 检出新建分支 branch（起点 HEAD）。
func AddWorktree(ctx context.Context, repo, path, branch string) error {
	return addWorktreeWith(ctx, runGit, repo, path, branch)
}

func addWorktreeWith(ctx context.Context, run gitRunner, repo, path, branch string) error {
	_, err := run(ctx, repo, "worktree", "add", path, "-b", branch)
	return err
}

// RemoveWorktree 移除隔离工作区；deleteBranch 时连带删除任务分支。
func RemoveWorktree(ctx context.Context, repo, path, branch string, deleteBranch bool) error {
	return removeWorktreeWith(ctx, runGit, repo, path, branch, deleteBranch)
}

func removeWorktreeWith(ctx context.Context, run gitRunner, repo, path, branch string, deleteBranch bool) error {
	if _, err := run(ctx, repo, "worktree", "remove", "--force", path); err != nil {
		return err
	}
	if deleteBranch {
		if _, err := run(ctx, repo, "branch", "-D", branch); err != nil {
			return err
		}
	}
	return nil
}

// CollectCommits 收集 base 之后的 commit 摘要行（git log --oneline base..HEAD）。
func CollectCommits(ctx context.Context, dir, base string) ([]string, error) {
	return collectCommitsWith(ctx, runGit, dir, base)
}

func collectCommitsWith(ctx context.Context, run gitRunner, dir, base string) ([]string, error) {
	out, err := run(ctx, dir, "log", "--oneline", base+"..HEAD")
	if err != nil {
		return nil, err
	}
	var commits []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			commits = append(commits, line)
		}
	}
	return commits, nil
}
