package session

import (
	"crypto/rand"
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
