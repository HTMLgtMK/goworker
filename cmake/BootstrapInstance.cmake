# BootstrapInstance：生成一个隔离的 goworker 运行时目录。
#
# 用法（经 `cmake --build build --target instance`）：
#   -DINSTANCE_DIR=<dir>   运行时目录，默认 build/instance
#
# 产出 <dir>/config.yaml（默认配置骨架）并打印启动命令。
# 多实例原理：GOWORKER_CONFIG_DIR 隔离各实例的 config/sessions 等运行时目录。

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
")
    message(STATUS "已生成 ${CONFIG_FILE}")
else()
    message(STATUS "配置已存在：${CONFIG_FILE}")
endif()

message(STATUS "启动该实例：")
message(STATUS "  GOWORKER_CONFIG_DIR=${INSTANCE_DIR} <goworker 二进制>")
