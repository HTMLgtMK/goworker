# Quickstart: Command Decision Engine

**Date**: 2026-08-17
**Scope**: 端到端验证指南。细节见 [contracts/](contracts/) 与 [data-model.md](data-model.md)。

## 前置条件

- Go 1.26+，`cd daemon`
- 迁移门禁：现有命令安全测试零改动通过
  ```bash
  go test ./internal/sandbox/... ./internal/plugins/agent/middlewares/...
  ```

## 验证场景

### 1. 结构化分级 + 决策矩阵（US1）

```bash
go test ./internal/sandbox/... -run 'Policy|Assess' -v
```

**命令级行为断言**（normal 模式）：

| 命令 | 预期 |
|---|---|
| `git status` / `ls` / `grep foo` | allow（不弹确认） |
| `rm -rf /` | deny（直接拒，不进确认） |
| `pip install foo` / `openssl x509 -text` | hitl（弹确认） |
| readonly 模式下的 `cat a > /tmp/b` | deny |

### 2. 解析强化 / 绕过封堵（US2）

```bash
go test ./internal/sandbox/... -run 'Normalize' -v
```

- `echo hi\nsudo rm -rf /tmp` → **不得 allow**（换行绕过已封）
- `$(curl ...)` / 反引号 / `bash -c 'rm -rf /'` → 升级 hitl
- `echo "$(date)"` → 保持 allow（引号内字面量不误报）

### 3. 预批准规则（US3）

```yaml
# ~/.config/goworker/config.yaml
sandbox:
  allow_rules:
    - match: "git push"
      max_risk: "R4"
      effects: ["network", "file_write"]
```

- `git push origin main` → allow（免确认）
- `git push --force origin` → hitl（destructive 超出 effects 子集）
- `git fetch` → 规则不命中，按常规评估

### 4. 审计（US4）

```yaml
sandbox:
  audit_log: true
```

启动 REPL 执行一次需要确认的命令并批准，检查：

```bash
cat ~/.config/goworker/audit/audit.jsonl
```

预期记录含：`risk_level`、`effects`、`engine_decision: hitl`、`user_decision: approve`、`outcome: executed`。

### 5. REPL 端到端

```bash
go run cmd/goworker/main.go
/model set endpoint=... model=...   # 配置 LLM
/agent                              # 进入代理模式，让它执行上面各类命令
```

逐条核对确认提示出现/不出现、风险标签 `[R4]` 是否渲染。

## 退出条件

- `go test -race ./internal/sandbox/... ./internal/plugins/agent/middlewares/...` 全绿
- 迁移门禁（旧测试零改动）通过
- 5 个场景逐一验证通过
