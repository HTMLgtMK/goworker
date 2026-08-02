package core

import "encoding/json"

// EstimateTokens 粗估消息列表的 token 消耗（JSON 字节数 / 4）。
// UsageMiddleware 的兜底估算与压缩预检共用，抽出来避免两处各算一遍。
//
// 注意：只算 messages 本体，不含随请求重发的工具 schema ——
// 用于压缩阈值判断时天然留了余量，0.8 阈值不会误伤。
func EstimateTokens(msgs []Message) int {
	b, err := json.Marshal(msgs)
	if err != nil {
		return 0
	}
	return len(b) / 4
}
