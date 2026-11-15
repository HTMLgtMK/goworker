# LLM 配置手动迁移

当前版本只接受新的 `llm.current` + `llm.providers.<name>.models` 配置结构。旧格式会被配置加载器拒绝，daemon 不会自动迁移、回退解析或改写旧配置；升级前请手动完成迁移。不要把旧字段留在文件里，哪怕新字段也已经添加，否则启动仍会失败。

## 安全准备

1. 停止 goworker 服务和所有使用同一配置目录的实例，避免它们在迁移期间写回配置。
2. 确认实际配置目录。默认是 `~/.config/goworker`；设置了 `GOWORKER_CONFIG_DIR` 时使用该目录。若启动脚本通过其他方式指定配置路径，请以实际路径为准。
3. 在配置文件所在目录创建仅当前用户可读的备份，并限制原文件和备份权限。下面的命令适用于默认或 `GOWORKER_CONFIG_DIR` 配置目录：

   ```sh
   set -eu
   CONFIG_DIR="${GOWORKER_CONFIG_DIR:-$HOME/.config/goworker}"
   CONFIG="$CONFIG_DIR/config.yaml"
   BACKUP="$CONFIG.pre-provider-migration-$(date +%Y%m%d%H%M%S)"
   test -f "$CONFIG" || { printf '配置文件不存在: %s\n' "$CONFIG" >&2; exit 1; }
   test ! -e "$BACKUP" || { printf '备份文件已存在: %s\n' "$BACKUP" >&2; exit 1; }
   umask 077
   cp -p "$CONFIG" "$BACKUP"
   chmod 600 "$CONFIG" "$BACKUP"
   printf '已备份到 %s\n' "$BACKUP"
   ```

   备份和配置文件都包含明文凭据。不要把它们放进代码仓库、同步到不可信位置、贴进工单或聊天，也不要通过命令行参数、shell 历史或终端录屏传递 API Key。用本机可信编辑器直接编辑配置；保留原有凭据时，只把它移动到对应的新 `api_key` 字段。确认配置文件及其目录由预期用户持有且没有其他用户可读权限。

4. 按下文映射重写整个 `llm` 区块。不要只追加新字段：必须删除旧字段，并检查每个 Provider 和模型。
5. 保存后先按“验证”步骤确认能正常启动和发起请求，再恢复服务。

## 旧字段映射

旧版扁平配置常见字段：

| 旧字段 | 新字段 | 说明 |
| --- | --- | --- |
| `llm.default_provider` | `llm.current.provider` | 值作为 Provider 注册名；对应条目必须存在于 `llm.providers`。 |
| `llm.model` | `llm.current.model`，以及所选 Provider 的 `models.<model-id>` 键 | Model ID 同时是当前选择和模型注册表的键。 |
| `llm.endpoint` | `llm.providers.<name>.endpoint` | 连接地址属于 Provider。 |
| `llm.api_key` | `llm.providers.<name>.api_key` | 凭据属于 Provider；原值不要写入文档或提交。 |
| `llm.context_window` | `llm.providers.<name>.models.<model-id>.context_window` | 上下文窗口属于具体模型。 |
| `llm.max_tokens` | `llm.providers.<name>.models.<model-id>.max_tokens` | 仅 Anthropic 模型使用；OpenAI 类型不能设置此字段。 |
| `llm.thinking.request_mode`、`llm.thinking.effort` | `llm.providers.<name>.models.<model-id>.thinking.*` | 仅 OpenAI 类型支持。全局 `llm.thinking.show` 仍留在 `llm.thinking.show`。 |
| `llm.compress_at`、`llm.compact_keep`、`llm.max_iterations`、`llm.thinking.show` | 字段名和位置不变 | 这些是全局策略，不移入 Provider 或 Model。 |

旧版 Provider 级字段也要下沉到模型条目：

| 旧字段 | 新字段 |
| --- | --- |
| `llm.providers.<name>.model` | `llm.current.model`（当它是当前 Provider 时），并创建 `llm.providers.<name>.models.<model-id>` |
| `llm.providers.<name>.context_window` | `llm.providers.<name>.models.<model-id>.context_window` |
| `llm.providers.<name>.max_tokens` | `llm.providers.<name>.models.<model-id>.max_tokens`（仅 Anthropic） |
| `llm.providers.<name>.thinking.request_mode`、`.thinking.effort` | `llm.providers.<name>.models.<model-id>.thinking.*`（仅 OpenAI） |
| `llm.providers.<name>.type`、`.endpoint`、`.api_key`、`.auth_type` | 保持 Provider 级；`type`、`endpoint` 必须明确且有效，`auth_type` 仅 Anthropic 使用。 |

### 单 Provider 扁平配置示例

旧配置（示意，凭据已省略）：

```yaml
llm:
  default_provider: openai
  endpoint: https://api.openai.com/v1
  api_key: "<原有凭据>"
  model: gpt-4o
  context_window: 128000
  thinking:
    request_mode: auto
    effort: medium
  compress_at: 0.8
```

手动改写为：

```yaml
llm:
  current:
    provider: openai
    model: gpt-4o
  compress_at: 0.8
  providers:
    openai:
      type: openai
      endpoint: https://api.openai.com/v1
      api_key: "<在本机编辑器中填入原有凭据>"
      models:
        gpt-4o:
          context_window: 128000
          thinking:
            request_mode: auto
            effort: medium
```

Provider 名 `openai` 必须与 `current.provider` 一致，`gpt-4o` 必须同时作为 `current.model` 和 `models` 的键。将示例中的凭据占位文本替换为原值；不要原样保留占位文本。

### Provider 级旧配置示例

旧配置中每个 Provider 直接持有 `model`、`context_window` 或 `thinking` 时，把它们移入对应模型 ID 下的 `models`。例如原有 Provider 名为 `openai`、模型 ID 为 `gpt-4o`，应整理为：

```yaml
llm:
  current:
    provider: openai
    model: gpt-4o
  providers:
    openai:
      type: openai
      endpoint: https://api.openai.com/v1
      api_key: "<在本机编辑器中填入原有凭据>"
      models:
        gpt-4o:
          context_window: 128000
          thinking:
            request_mode: auto
            effort: medium
```

如果旧 Provider 上没有 `type`，根据实际 API 协议填写 `openai` 或 `anthropic`，不要仅凭服务商名称猜测。对于有多个模型的 Provider，分别建立多个 `models.<model-id>` 条目；只有 `current` 指定的 Provider 和模型会被选中。

## 多 Provider 与 Anthropic

Provider 注册名是本地配置标识，`current.provider` 必须引用其中一个名字。`current.model` 必须存在于该 Provider 的 `models` 中。提供 `providers` 后它会替换内置注册表，不会与内置项合并；每个注册 Provider 都必须配置有效的 `type`、`endpoint` 和至少一个模型，即使当前没有选中它。

下面是 OpenAI-compatible 和 Anthropic Provider 并存的结构示例。请用实际模型 ID、窗口/token 限额和凭据替换占位值：

```yaml
llm:
  current:
    provider: openai
    model: gpt-4o
  providers:
    openai:
      type: openai
      endpoint: https://api.openai.com/v1
      api_key: "<OpenAI 凭据>"
      models:
        gpt-4o:
          context_window: 128000
          thinking:
            request_mode: auto
            effort: medium
    anthropic:
      type: anthropic
      endpoint: https://api.anthropic.com/v1
      auth_type: x-api-key
      api_key: "<Anthropic 凭据>"
      models:
        claude-sonnet-4-5:
          context_window: 200000
          max_tokens: 8192
```

Anthropic Provider 必须有非空 `api_key`、有效 `endpoint`，并显式设置 `auth_type: x-api-key` 或 `auth_type: bearer`。每个 Anthropic 模型都必须配置正数 `context_window` 和 `max_tokens`；Anthropic 模型不能带 OpenAI 的 `thinking.request_mode` / `thinking.effort`。OpenAI 类型不接受 `auth_type` 或模型级 `max_tokens`；其模型 `context_window`、thinking 方言可省略并使用默认值，也可按模型填写。不要把 Anthropic 的 HTTP 认证头类型与模型协议混淆，`type` 应与实际 endpoint 支持的协议相符。

## 验证与回滚

1. 启动之前检查 YAML 缩进、引号和字段位置；确认文件中已无 `default_provider`、扁平 `endpoint`/`model`/`api_key`/`context_window`/`max_tokens`、`llm.thinking.request_mode`/`effort`，也无 Provider 级 `model`/`context_window`/`max_tokens`/`thinking`。
2. 用原来的启动方式启动 goworker。配置无法解析或校验失败时 daemon 不应带默认配置继续运行；检查本机错误日志，但不要复制包含凭据的配置或日志到公开渠道。
3. 通过 `/config` 查看加载后的配置。该命令会对 API Key 脱敏；确认 `current` 和 Provider/Model 注册表正确，不能通过显示明文凭据来验证密钥。
4. 发起一条无副作用的普通请求，确认所选 Provider 能通过认证并返回响应。若要验证另一个 Provider，先按正常方式切换 `current`（或使用应用已有的模型切换命令），再做同样的请求；不要用有破坏性的工具调用做连通性测试。
5. 观察确认请求成功后再恢复其他实例。若出现启动或请求问题，停止 goworker，在同一目录把备份原子恢复为 `config.yaml`，再以原启动方式验证旧版本/原配置：

   ```sh
   set -eu
   CONFIG_DIR="${GOWORKER_CONFIG_DIR:-$HOME/.config/goworker}"
   CONFIG="$CONFIG_DIR/config.yaml"
   BACKUP="/替换为上面显示的完整备份路径"
   test -f "$BACKUP" || { printf '备份文件不存在: %s\n' "$BACKUP" >&2; exit 1; }
   test ! -e "$CONFIG.rollback" || { printf '回滚临时文件已存在: %s\n' "$CONFIG.rollback" >&2; exit 1; }
   umask 077
   cp "$BACKUP" "$CONFIG.rollback"
   chmod 600 "$CONFIG.rollback"
   mv "$CONFIG.rollback" "$CONFIG"
   ```

   `mv` 与目标文件位于同一目录，可用原子重命名替换配置。回滚会恢复旧格式文件；如果当前二进制只接受新格式，旧格式仍然无法加载。要运行旧二进制时再恢复旧配置；要继续使用当前版本，则修正新格式迁移后重启。保留备份直到验证完成，然后按组织的凭据保留策略安全删除。
