# Research

## Transport

VS Code WebView 不能访问本地 Unix socket。extension host 使用 Node `net.Socket`，通过与 Go `protocol.Conn` 相同的 newline-delimited JSON-RPC 2.0 帧通信。socket 保持本地路径，不接受 TCP URL。

## Data sources

- `--console/tasks`：一次性 `goworker_task_list` custom update，供 sidebar 使用。
- `--console/task <id>`：一次性 `goworker_task_detail` custom update，供 sticky header 使用。
- `--attach <id>`：原生 ACP update 回放与 live tail，供 archaeology trace 使用。

不能解析 `--ls`、`--status`，它们是人类可读命令输出，不是 API。

## Rendering safety

初版用 `textContent` 和 `<pre>` 渲染 message/thought，未知事件只显示 JSON 文本。CSP 禁止网络、内联脚本与任意 HTML injection。以后接 Markdown parser 也必须先走 sanitizer。
