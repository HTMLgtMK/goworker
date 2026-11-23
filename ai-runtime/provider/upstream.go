package provider

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

// upstreamErrorLimit caps how many bytes of an upstream error body we read before
// giving up on extracting a structured message. Error bodies are diagnostics and
// should stay small; the cap keeps a misbehaving gateway from dumping unbounded data.
const upstreamErrorLimit = 4096

// upstreamErrorMaxLen is the rune budget for the message surfaced to the caller.
// It is sized for one-line UI rendering; the full upstream message still reaches logs
// through the structured side of UpstreamErrorDetail.
const upstreamErrorMaxLen = 300

// UpstreamErrorDetail extracts a structured diagnostic message from an upstream error
// response. The raw body never leaves this function: it may echo the request (user
// prompt / tool inputs), so only the parsed message field is surfaced — to the caller
// as a rune-safe truncated string, and to logs as structured data. The provider name
// is only used for log attribution.
func UpstreamErrorDetail(resp *http.Response, name string) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, upstreamErrorLimit))
	if len(body) == 0 {
		return ""
	}
	var wire struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		// 非 JSON 错误体：不把原始内容回传（可能回显请求），只留结构化日志。
		slog.Warn("provider: upstream error body is not JSON", "provider", name, "status", resp.StatusCode)
		return ""
	}
	message := wire.Error.Message
	if message == "" {
		message = wire.Message
	}
	message = strings.TrimSpace(message)
	if message == "" {
		slog.Warn("provider: upstream error without message", "provider", name, "status", resp.StatusCode)
		return ""
	}
	if r := []rune(message); len(r) > upstreamErrorMaxLen {
		message = string(r[:upstreamErrorMaxLen]) + "…"
	}
	slog.Warn("provider: upstream API error", "provider", name, "status", resp.StatusCode, "message", message)
	return message
}
