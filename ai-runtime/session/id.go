package session

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// NewMsgID 返回 m_ 开头的 8 位 hex 随机 id。
// 用 crypto/rand 读 4 字节，出错时回退到时间戳生成的 id。
// 供 store 副本生成与 agent 包消息盖章共用（跨包唯一入口，避免实现漂移）。
func NewMsgID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 极少失败，但万一系统熵池干了就回退
		return fmt.Sprintf("m_%016x", time.Now().UnixNano())
	}
	return "m_" + hex.EncodeToString(b)
}

// NewSessionID returns a random logical session identity. It is deliberately
// independent from message/head IDs: compact and rewind are allowed to replace
// the latter without changing the session resource.
func NewSessionID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("session_%016x", time.Now().UnixNano())
	}
	return "session_" + hex.EncodeToString(b)
}

// legacySessionID creates a deterministic identity for a JSONL file written by
// versions that predate metadata. A stable first node ID is preferred because
// it remains unchanged when current.jsonl is archived; callers use the content
// digest only when the legacy file has no node records.
func legacySessionID(firstNodeID string, content []byte) string {
	seed := firstNodeID
	if seed == "" {
		digest := sha256.Sum256(content)
		seed = hex.EncodeToString(digest[:])
	}
	digest := sha256.Sum256([]byte(seed))
	return "legacy_" + hex.EncodeToString(digest[:8])
}
