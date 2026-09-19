# goworker 构建编排：CMake 驱动 Go toolchain（go.work 单仓多模块）。
#
# 用法：
#   cmake -B build && cmake --build build          # 构建 bin/goworker
#   cmake --build build --target test              # 全量 go test
#   cmake --build build --target vet               # go vet
#   cmake --build build --target run               # 构建并启动 REPL
#   cmake --install build --prefix /usr/local      # 安装到 <prefix>/bin
#
# 交叉编译：
#   cmake -B build -DGOOS=linux -DGOARCH=amd64 && cmake --build build
#
# 版本号：git describe（tags 优先）注入 ldflags -X internal/version.version，
# 运行 `goworker -version` 查询；无 tag 时回退 project version。
cmake_minimum_required(VERSION 3.20)

# 禁止源码内构建（Go 源码树混入 CMake 产物会污染 go build 的目录遍历）
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR "请使用 out-of-source 构建：cmake -B build")
endif()

project(goworker
    VERSION 0.1.0
    DESCRIPTION "goworker - modular REPL agent terminal"
    LANGUAGES NONE
)

find_program(GO_EXECUTABLE go REQUIRED)
find_program(GIT_EXECUTABLE git)

# ---- 参数 ----
set(GOOS "" CACHE STRING "交叉编译目标 GOOS（空 = 本机）")
set(GOARCH "" CACHE STRING "交叉编译目标 GOARCH（空 = 本机）")
set(GO_BUILD_FLAGS "-trimpath" CACHE STRING "附加 go build 参数")
set(GOWORKER_PLUGINS "agent" CACHE STRING
    "编译进 daemon 的插件，逗号/分号分隔（可选：agent）。未选中的插件经 goworker_no_<name> 构建标签编译期剔除")

# ---- 版本号 ----
set(PROJ_VERSION ${PROJECT_VERSION})
if(GIT_EXECUTABLE AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} describe --tags --always --dirty
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        RESULT_VARIABLE GIT_RESULT
    )
    if(GIT_RESULT EQUAL 0 AND GIT_VERSION)
        set(PROJ_VERSION ${GIT_VERSION})
    endif()
endif()
message(STATUS "goworker version: ${PROJ_VERSION}")

# ---- 模块清单（从 go.work 动态读取，与 ci.yml 同源，避免硬编码清单漂移）----
file(STRINGS ${CMAKE_CURRENT_SOURCE_DIR}/go.work _gowork_lines
    REGEX "^[ \t]*\\./[a-zA-Z-]+[ \t]*$")
set(GO_MODULES "")
foreach(_line IN LISTS _gowork_lines)
    string(REGEX REPLACE "^[ \t]*\\./([a-zA-Z-]+)[ \t]*$" "\\1" _mod "${_line}")
    list(APPEND GO_MODULES "${_mod}")
endforeach()
set(GO_TEST_PKGS "")
foreach(_mod IN LISTS GO_MODULES)
    list(APPEND GO_TEST_PKGS "./${_mod}/...")
endforeach()
message(STATUS "go.work 模块: ${GO_MODULES}")

# ---- 插件装配（GOWORKER_PLUGINS → go build tags）----
# 语义是「全量默认 + 按清单剔除」：不带 tag 的裸 go build（CI/release/README 流程）
# 始终编入全部插件；CMake 只对未选中的插件追加 goworker_no_<name> off-tag。
# Go 侧每个插件对应一对 plugin_<name>.go / plugin_<name>_off.go（见 daemon/cmd/goworker）。
set(_known_plugins "agent")
string(REPLACE "," ";" _plugin_list "${GOWORKER_PLUGINS}")
set(_selected_plugins "")
foreach(_p IN LISTS _plugin_list)
    string(STRIP "${_p}" _p)
    if(_p STREQUAL "")
        continue()
    endif()
    if(NOT _p IN_LIST _known_plugins)
        message(FATAL_ERROR "GOWORKER_PLUGINS 含未知插件 '${_p}'，可选值：${_known_plugins}")
    endif()
    list(APPEND _selected_plugins "${_p}")
    list(REMOVE_ITEM _known_plugins "${_p}")
endforeach()
set(GO_OFF_TAGS "")
foreach(_p IN LISTS _known_plugins)
    list(APPEND GO_OFF_TAGS "goworker_no_${_p}")
endforeach()
set(_go_tag_args "")
if(GO_OFF_TAGS)
    list(JOIN GO_OFF_TAGS "," _off_tag_str)
    set(_go_tag_args "-tags" "${_off_tag_str}")
endif()
if(_selected_plugins)
    message(STATUS "plugins: 编入 ${_selected_plugins}（剔除：${GO_OFF_TAGS}）")
else()
    message(STATUS "plugins: 最小壳，不编入任何插件")
endif()

# ---- 输出 ----
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(GOWORKER_BIN_NAME goworker)
if(GOOS OR GOARCH)
    string(TOLOWER "${GOOS}-${GOARCH}" _cross_suffix)
    set(GOWORKER_BIN_NAME "goworker-${_cross_suffix}")
endif()
set(GOWORKER_BIN ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${GOWORKER_BIN_NAME})

# ---- 源文件清单（增量重建触发器）----
file(GLOB_RECURSE GO_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*.go
    ${CMAKE_CURRENT_SOURCE_DIR}/go.work
    ${CMAKE_CURRENT_SOURCE_DIR}/*/go.mod
    ${CMAKE_CURRENT_SOURCE_DIR}/*/go.sum
)

# ---- 构建 ----
set(_go_env "GOOS=${GOOS}" "GOARCH=${GOARCH}")
add_custom_command(
    OUTPUT ${GOWORKER_BIN}
    COMMAND ${CMAKE_COMMAND} -E env ${_go_env}
        ${GO_EXECUTABLE} build ${GO_BUILD_FLAGS} ${_go_tag_args}
            -ldflags "-X github.com/tinguo/goworker/daemon/internal/version.version=${PROJ_VERSION}"
            -o ${GOWORKER_BIN}
            ./daemon/cmd/goworker
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${GO_SOURCES}
    COMMENT "go build -> ${GOWORKER_BIN_NAME} (${PROJ_VERSION})"
    VERBATIM
)
add_custom_target(goworker ALL DEPENDS ${GOWORKER_BIN})

# ---- 辅助目标 ----
add_custom_target(test
    COMMAND ${GO_EXECUTABLE} test ${GO_TEST_PKGS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "go test（全 workspace）"
    VERBATIM
)

add_custom_target(test-race
    COMMAND ${GO_EXECUTABLE} test -race ./...
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "go test -race"
    VERBATIM
)

add_custom_target(vet
    COMMAND ${GO_EXECUTABLE} vet ${GO_TEST_PKGS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "go vet"
    VERBATIM
)

# run：先构建再启动 REPL（交互式，不作为 ALL 依赖）
add_custom_target(run
    COMMAND ${GOWORKER_BIN}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS goworker
    COMMENT "启动 goworker REPL"
    VERBATIM
)

# 多实例辅助：生成一个隔离开的运行时目录并提示启动命令
# （GOWORKER_CONFIG_DIR 隔离 config/sessions/memory/dispatch/acp.sock）
add_custom_target(instance
    COMMAND ${CMAKE_COMMAND}
        -DINSTANCE_DIR=${CMAKE_BINARY_DIR}/instance
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/BootstrapInstance.cmake
    COMMENT "准备隔离子运行时目录 build/instance"
    VERBATIM
)

# ---- 安装 ----
install(PROGRAMS ${GOWORKER_BIN} DESTINATION bin RENAME goworker)
