# Design: 命令 autocomplete（Tab 按需补全）

**Date**: 2026-09-06 | **Status**: 设计定稿，未实现
**交互形态**: Tab 按需补全（zsh/fish 风格，用户已选定；非实时弹层）

## 1. 目标与非目标

**目标**
- 斜杠命令名的前缀补全：`/co` + Tab → `/compact`，唯一候选直接补全，多候选循环切换。
- 候选可发现性：Tab 展示候选列表（命令名 + 描述），不再依赖 `/help` 记忆命令。

**非目标（本期不做）**
- 实时弹出候选菜单（输入即过滤、↑↓ 选择）——已否决，选 Tab 按需。
- 参数级补全（Phase 2 设计见 §6，本期只出设计不实现）。
- 模糊匹配（子序列/fuzzy）——首版严格前缀，模糊留 Phase 3。
- 中文输入法组合期的补全（raw mode 下 IME 组合态无法可靠探测，跳过）。

## 2. 交互规格

| 场景 | 行为 |
|---|---|
| 输入以 `/` 开头、光标在第一个词内、按 Tab | 计算候选（见 §3）。无候选：终端响铃（`\a`），无其他动作。唯一候选：直接补全为该命令名 + 尾随空格。多候选：进入 cycle 模式，先补全为排序第一个候选，列表临时展示在输入行下方 |
| cycle 模式中再按 Tab | 补全切换到下一个候选（循环）；Shift-Tab 反向 |
| cycle 模式中任意编辑键（字符/退格/方向键） | 退出 cycle 模式，撤下候选列表，正常编辑 |
| Enter / Esc / ↑↓ 历史 | 不受影响：Enter 提交当前行（同时退出 cycle），Esc 走既有取消链（keyWatcher），↑↓ 仍是历史导航 |
| 光标不在第一个词内、或行不以 `/` 开头 | Tab 无动作（暂不响铃，避免噪音） |

**补全结果形态**：只补命令名本身（如 `/compact`），尾随一个空格，光标落词尾。别名不参与直接补全（补全 canonical 名），但参与候选匹配（`/l` + Tab 时 `/llm` 作为 `/agent` 的别名出现在候选里，补全为 `/agent`）。

## 3. 数据源与匹配

- 来源：`engine.Commands()`（`daemon/internal/core/engine.go:190`），frontend 在 `Run()` 启动时拉取一次缓存。命令集在运行期不变（全部在 Init 注册），无需失效机制。
- 候选项：`{completion string, description string}`；`Commands()` 已按 Name 去重，别名归并：对每个命令，其 `Aliases` 中前缀匹配的项以 `alias → canonical` 形式入候选（completion 取 canonical）。
- 匹配：大小写不敏感前缀匹配（命令名全小写）。`/` 后为空 → 列出全部。
- 排序：字典序稳定排序（`Commands()` 的 map 迭代序随机，必须显式排序）。

## 4. 键盘管线改动

### 4.1 解码层（`daemon/internal/frontend/stdin/key.go`）

- 新增 `KeyType`：`KeyTab`（0x09）、`KeyShiftTab`（CSI `ESC [ Z`）。
- `process()`：当前 `b >= 0x20` 才进字符累积，0x09 落空被丢弃 → 新增 `case b == 0x09: emit(KeyTab)`。
- `finishCSI()`：新增 `case 'Z'`（在 esc3 状态机之外，与 'A'~'D' 平行）：`ESC [ Z → KeyShiftTab`。

### 4.2 编辑器（`daemon/internal/frontend/stdin/editor.go`）

- `Consume()`：`KeyTab`/`KeyShiftTab` 加入消费键列表（仅 active 会话时）。
- `ReadLine()`：新增 Tab 分支 → 调用 `autocomplete` 模块（新文件 `autocomplete.go`）：
  - 输入：当前 buf、pos、候选缓存。
  - 输出：新的 buf/pos（补全后）+ 是否进入/维持 cycle + 候选列表（渲染用）。
- editor 持有 `*completer`（见 §5），状态（cycle 索引、候选列表）随每次 `ReadLine` 开始重置。

### 4.3 渲染

- 候选列表渲染在输入行下方（输入行内容 + 候选多行），复用 `redrawInput()` 的多行擦除机制（`renderedRows` 已支持多行光标回溯）：
  - `redrawInput` 改为绘制 `prompt + buf` 后追加候选块（cycle 激活时），擦除行数 = 输入行 + 候选行数。
  - 候选行格式：`命令名  描述`，描述用灰色弱化（复用 thinking 的 `\033[38;5;244m`）；当前 cycle 指向的候选高亮（反白或青色）。
  - 候选超过终端剩余行数时截断 + `… (+N more)` 计数提示。
- 与 statusbar 互不干扰：候选块属于输入区，由 editor 自管擦除；statusbar 在底部独立清/绘（`Write()` 已有 WithLock 协议）。但注意：cycle 激活期间若 statusbar 在刷（agent 不可能同时跑，主 goroutine 串行——实际不冲突）。

## 5. 模块边界

新建 `daemon/internal/frontend/stdin/autocomplete.go`，纯逻辑、可表驱动测试：

```go
type candidate struct {
    completion  string // 补全结果（canonical 命令名）
    description string
}

type completer struct {
    candidates []candidate          // 启动时从 engine.Commands() 缓存，已排序
    cycling    bool
    cycleIdx   int
    cycleFrom  string               // 进入 cycle 时的原始输入（Shift-Tab 回退基准）
}

// complete 处理一次 Tab/Shift-Tab。返回补全后的文本与是否展示候选列表。
func (c *completer) complete(input string, forward bool) (text string, showList []candidate)
```

editor 只做按键分派与渲染；匹配/循环状态机全部在 completer，单测不需要终端。

## 6. Phase 2（参数补全，本期仅设计）

- `plugin.Command` 增加可选接口（不改现有构造，向后兼容）：

```go
type Completer interface {
    // Complete 返回当前参数位的候选。args 已含已输入的参数（不含命令名）。
    Complete(args []string) []string
}
```

- `core.Engine` 增加 `Complete(name string, args []string) []string`：查表 → 断言 Completer → 返回候选；frontend 在第一个词已完整（buf 中光标前含空格）时 Tab 走此路径。
- 首批接入：`/model use <provider>`（providers 列表）、`/config <key>`（配置键）、`/compact`/`/new`（无参数，提示空）。
- 候选展示与命令名补全共用 cycle 机制；参数候选不做前缀强制（列出即可，Tab 循环选中回填）。

## 7. Phase 3（可选增强）

- 输入历史持久化到 `~/.config/goworker/history`（现有 `editor.go` history 仅内存 50 条）。
- Ctrl+R 历史模糊搜索（需新增 `KeyCtrlR` 解码 + 交互态，与 cycle 模式互斥）。
- 命令名模糊匹配（子序列）——仅在前缀无候选时降级启用。

## 8. 测试策略

- `key_test.go`：Tab（0x09）、Shift-Tab（`ESC [ Z`）解码单测；Tab 与 Ctrl+I 歧义（raw mode 下就是同一字节，无歧义）。
- `autocomplete_test.go`：表驱动——前缀匹配、别名归并、唯一候选直接补全、多候选 cycle 顺序与回绕、Shift-Tab 反向、编辑退出 cycle、无候选响铃不改文本。
- `editor_test.go`：Tab 分支后 buf/pos/renderedRows 一致性（候选行计入擦除范围）。
- 手动验收：`/c` + Tab 循环 `/config`/`/compact`；`/q` + Tab 直接补全；候选列表与 statusbar 并存时 resize 不错位。
