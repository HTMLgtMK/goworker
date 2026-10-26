//go:build unix

// lock_unix.go 实现会话目录的跨进程文件锁（unix 族：linux/darwin/android/bsd…）。
// 语义对应总方案第九节「多进程同时打开同一 Session 时拒绝第二个写者」：
//
//   - 每个会话目录一把锁：目录下常驻 lock 文件（O_CREATE|O_RDWR, 0600）+
//     flock(LOCK_EX|LOCK_NB) 非阻塞尝试，被占（含 NFS/SMB 上 flock 不可用或
//     语义退化为本地锁的场景——失败即拒绝，宁拒勿双写）→ ErrStoreLocked；
//   - flock 以 open file description 为界：同进程对同一目录的第二个句柄同样
//     抢不到锁，因此进程内直接双开也被拒（进程内唯一由 Repository 缓存保证，
//     见 repository.go Open 的串行慢路径）；
//   - 锁文件常驻不删除：unlink 后另一进程可能正打开旧 inode 并对它加锁，两个
//     进程各持不同 inode 的锁、互斥彻底失效（unlink 竞态）。
package session

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// acquireDirLock 对会话目录 dir 加排它文件锁。成功返回锁句柄（由 Store 持有，
// Close 时经 releaseDirLock 释放）；失败（已被占用 / flock 不可用）返回包裹
// ErrStoreLocked 的错误。
func acquireDirLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, "lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("session: open lock file %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("session: lock %s: %w: %w", path, ErrStoreLocked, err)
	}
	return f, nil
}

// releaseDirLock 解锁并关闭锁句柄。锁文件本身保留在目录中（见文件头 unlink 竞态）。
func releaseDirLock(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		f.Close()
		return fmt.Errorf("session: unlock: %w", err)
	}
	return f.Close()
}
