package agent

import (
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/session"
)

// stampMessageIDs 给消息历史盖章：空 MsgID 补 session.NewMsgID()，零值 CreatedAt 补 time.Now()。
// 已有 id/时间的消息保留不动。返回新 slice，不原地改。
func stampMessageIDs(msgs []core.Message) []core.Message {
	out := make([]core.Message, len(msgs))
	for i, m := range msgs {
		out[i] = m
		if out[i].MsgID == "" {
			out[i].MsgID = session.NewMsgID()
		}
		if out[i].CreatedAt.IsZero() {
			out[i].CreatedAt = time.Now()
		}
	}
	return out
}
