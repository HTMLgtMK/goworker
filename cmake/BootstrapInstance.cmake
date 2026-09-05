# BootstrapInstance：生成一个隔离的 goworker 运行时目录。
#
# 用法（经 `cmake --build build --target instance`）：
#   -DINSTANCE_DIR=<dir>   运行时目录，默认 build/instance
#
# 产出 <dir>/config.yaml（默认配置骨架）并打印启动命令。
# 多实例原理：GOWORKER_CONFIG_DIR 隔离 config/sessions/memory/dispatch/acp.sock。

if(NOT INSTANCE_DIR)
    set(INSTANCE_DIR "${CMAKE_BINARY_DIR}/instance")
endif()

file(MAKE_DIRECTORY ${INSTANCE_DIR})
set(CONFIG_FILE ${INSTANCE_DIR}/config.yaml)

if(NOT EXISTS ${CONFIG_FILE})
    file(WRITE ${CONFIG_FILE} "# goworker 实例配置（GOWORKER_CONFIG_DIR 隔离）
log:
  level: info
  file: ${INSTANCE_DIR}/goworker.log

session:
  enabled: true

# 启用 dispatcher 时打开：
# dispatch:
#   enabled: true
#   default_worker: claude
#   max_parallel: 2
#   workers:
#     - name: claude
#       command: npx
#       args: [\"-y\", \"@zed-industries/claude-agent-acp\"]
#     - name: zcode
#       command: goworker
#       args: [\"acp\"]
#       env: [\"GOWORKER_CONFIG_DIR=<另一个实例的目录>\", \"GOWORKER_SANDBOX_MODE=strict\"]
#   routes:
#     - keywords: [\"测试\", \"review\"]
#       worker: zcode
")
    message(STATUS "已生成 ${CONFIG_FILE}")
else()
    message(STATUS "配置已存在：${CONFIG_FILE}")
endif()

message(STATUS "启动该实例：")
message(STATUS "  GOWORKER_CONFIG_DIR=${INSTANCE_DIR} <goworker 二进制>")
message(STATUS "worker 模式（被其他实例调度）：")
message(STATUS "  GOWORKER_CONFIG_DIR=${INSTANCE_DIR} <goworker 二进制> acp")
