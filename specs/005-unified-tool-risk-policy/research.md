# Research: Unified Tool Risk Policy

**Date**: 2026-09-27

## Decision 1: 新建 005，不改写 001

`001-command-decision-engine` 已经建立了 bash 命令引擎、`Check` 兼容 shim、旧 `allow_rules` 和审计语义。当前需求会改变跨工具 metadata、ModeAuto、critical 决策和 ACP permission 能力，直接编辑 001 会让已完成的命令引擎设计失去历史边界。

本 feature 以 001 为依赖，只在必要处新增兼容性说明或 errata。

## Decision 2: RiskLevel 与硬拒分离

当前实现把 denylist 命令标成 critical，并在 policy matrix 中把 critical 直接映射为 deny。这同时表达了“风险很高”和“绝不能执行”两个概念，导致普通 critical 工具无法进入 HITL。

采用独立字段：

- `RiskLevel=critical`：critical，可 HITL。
- `HardDeny=true`：当前 policy 不允许执行，不进入 HITL。
- deny pattern 产生 `HardDeny=true`；risky pattern 不产生。

拒绝原因仍保留稳定 reason code，例如 `deny_pattern`，但策略不能仅依赖等级数字判断硬拒。

## Decision 3: 外部 metadata 缺失默认 unclassified

外部工具的风险声明不是分类器的可靠证明。缺少或无法解析的风险等级视为 unclassified（unclassified），不得在 auto 模式因为“没有风险信息”而自动放行。

工具 metadata 通过 `core.Tool` 在 runtime 内部传递，但 provider 序列化工具定义时不发送策略字段，避免模型修改或伪造授权元数据。

## Decision 4: auto 是阈值策略，不是无条件放行

新增 `auto_max_risk`，建议默认 low：

```text
risk <= auto_max_risk  → allow
risk > auto_max_risk   → HITL
没有 responder         → reject
```

这样 `auto` 仍然允许 critical 进入 HITL。无人值守调用链如果没有 responder，会按 fail-closed 处理；不能把无法询问用户解释成自动批准。

## Decision 5: sys 的旧 risk_level 是策略，不是风险

当前 client-ward descriptor 使用 `never/mode/always`，它描述的是执行策略，不是 benign-reserved 风险。新契约统一 `risk_level` 为 benign-reserved，并将旧 descriptor 放进显式 migration adapter：

| 旧值 | 兼容映射 | 说明 |
|---|---|---|
| `never` | low + no permission gate | 保留“声明方担保无害”的旧放行行为 |
| `mode` | unclassified + normal policy gate | 无法从旧值推导真实风险，安全落未知 |
| `always` | critical + forced HITL policy | 保留“即使 off 也要问”的旧语义；strict/readonly 仍拒绝 |

新注册工具不得继续写入这三个字符串。若协议版本不支持兼容转换，必须明确报错而不是静默降级成 benign。

## Decision 6: allow_always 是用户状态，不是风险分类

风险等级决定系统默认策略，allow_always 表示用户对某个工具授权身份的明确批准。二者不能互相代替。

外部工具声明稳定、非敏感的 `allow_always_key`；持久化记录保存 key 和批准时风险上限。工具风险上升后旧批准不应自动复用。默认不序列化原始 tool args，避免配置泄漏 secret、路径和业务数据。

bash 继续使用 001 的命令 token `AllowRule`；外部工具使用独立 `ToolAllowRule`，不在 bash 规则上增加 ToolName 字段。

## Decision 7: 外部工具采用显式接入

当前 middleware 只识别 bash、`mcp_*`、`sys_*`，其他工具直通。直接把所有非 bash 工具纳入会改变 read_file/profile/memory 等内部工具行为。

采用：

- MCP/sys：桥接层自动标记为受策略控制。
- hub 注册工具：注册方显式设置 `requires_permission=true`。
- skill/memory/profile/default read-only 工具：没有声明则保持当前行为。

包装工具时必须复制 metadata，避免在 `model.Tool → core.Tool` 转换时丢掉策略声明。

## Rejected Alternatives

### 把 ToolName 加进 bash AllowRule

会让 shell 命令规则同时承担外部工具授权，污染 `sandbox` 的领域模型；未来匹配参数或 scope 时还会继续堆字段。

### 只把所有非 bash 工具都展示 allow_always

会误伤内部工具，且不能区分工具是否有稳定授权身份。

### 把完整 args JSON 作为永久授权 key

参数可能含 secret，且 JSON 顺序、非关键参数和默认值会导致授权不稳定。当前 feature 只支持工具声明的稳定 key。

### 把 critical 继续映射 deny

无法满足 critical 工具进入 HITL，也会把风险分类和硬拒继续绑死。

### 在 auto 模式超过阈值直接静默执行

会让 `auto_max_risk` 失去意义；超阈值必须进入 HITL，缺少 responder 时拒绝。

## Open Questions Resolved for Implementation

- auto 阈值比较使用 `<=`。
- 缺失/非法外部风险默认为 unclassified。
- normal 模式 benign/low allow，medium-reserved HITL。
- critical 可以 HITL；denylist 通过独立 hard-deny 保持拒绝。
- strict/readonly 永远优先于 allow_always。
- off 的旧 bypass 语义保留，但 hard-deny 在 off 下是否继续绕过必须由实现契约显式固定并测试。
