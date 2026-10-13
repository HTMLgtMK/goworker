# goworker 构建编排：CMake 驱动 Go toolchain（go.work 单仓多模块）。
#
# 用法：
#   cmake -B build && cmake --build build          # 构建兼容的 bin/goworker（默认 full）
#   cmake --build build --target goworker-agent    # 构建 Agent-only 产物
#   cmake --build build --target goworker-dispatcher
#   cmake --build build --target goworker-minimal
#   cmake --build build --target test               # 按 GOWORKER_NO_PLUGINS 执行全量 go test
#   cmake --build build --target test-agent         # 执行 Agent-only go test
#   cmake --build build --target vet                # 按 GOWORKER_NO_PLUGINS 执行 go vet
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
    VERSION 0.1.5
    DESCRIPTION "goworker - modular REPL agent terminal"
    LANGUAGES NONE
)

find_program(GO_EXECUTABLE go REQUIRED)
find_program(GIT_EXECUTABLE git)

# ---- 参数 ----
set(GOOS "" CACHE STRING "交叉编译目标 GOOS（空 = 本机）")
set(GOARCH "" CACHE STRING "交叉编译目标 GOARCH（空 = 本机）")
set(GO_BUILD_FLAGS "-trimpath" CACHE STRING "附加 go build 参数")
set(GOWORKER_NO_PLUGINS "" CACHE STRING
    "不编入 daemon 的插件，逗号/分号分隔（可选：agent、dispatcher）。默认空 = 全部编入；每个名字经 goworker_no_<name> 构建标签编译期剔除")

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

# ---- 插件装配（GOWORKER_NO_PLUGINS → go build tags）----
set(_known_plugins "agent" "dispatcher")
string(REPLACE "," ";" _exclude_list "${GOWORKER_NO_PLUGINS}")
set(GO_OFF_TAGS "")
foreach(_p IN LISTS _exclude_list)
    string(STRIP "${_p}" _p)
    if(_p STREQUAL "")
        continue()
    endif()
    if(NOT _p IN_LIST _known_plugins)
        message(FATAL_ERROR "GOWORKER_NO_PLUGINS 含未知插件 '${_p}'，可选值：${_known_plugins}")
    endif()
    list(APPEND GO_OFF_TAGS "goworker_no_${_p}")
endforeach()

function(_go_tag_args tags out_var)
    set(_args "")
    if(tags)
        list(JOIN tags "," _tag_string)
        set(_args "-tags" "${_tag_string}")
    endif()
    set(${out_var} "${_args}" PARENT_SCOPE)
endfunction()

_go_tag_args("${GO_OFF_TAGS}" _legacy_tag_args)
list(LENGTH GO_OFF_TAGS _off_count)
list(LENGTH _known_plugins _known_count)
if(_off_count EQUAL 0)
    message(STATUS "legacy goworker: 全量编入")
elseif(_off_count EQUAL _known_count)
    message(STATUS "legacy goworker: 最小壳，全部插件已剔除")
else()
    message(STATUS "legacy goworker: 剔除 ${GO_OFF_TAGS}，其余全量编入")
endif()

# ---- 输出 ----
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(_cross_suffix "")
if(GOOS OR GOARCH)
    string(TOLOWER "-${GOOS}-${GOARCH}" _cross_suffix)
endif()

# ---- 源文件清单（增量重建触发器）----
file(GLOB_RECURSE GO_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*.go
    ${CMAKE_CURRENT_SOURCE_DIR}/go.work
    ${CMAKE_CURRENT_SOURCE_DIR}/*/go.mod
    ${CMAKE_CURRENT_SOURCE_DIR}/*/go.sum
)

function(add_goworker_binary target_name output_stem all_target)
    set(_tags "${ARGN}")
    _go_tag_args("${_tags}" _tag_args)
    set(_output "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${output_stem}")
    set(_go_env "GOOS=${GOOS}" "GOARCH=${GOARCH}")
    add_custom_command(
        OUTPUT ${_output}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND ${CMAKE_COMMAND} -E env ${_go_env}
            ${GO_EXECUTABLE} build ${GO_BUILD_FLAGS} ${_tag_args}
                -ldflags "-X github.com/tinguo/goworker/daemon/internal/version.version=${PROJ_VERSION}"
                -o ${_output}
                ./daemon/cmd/goworker
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS ${GO_SOURCES}
        COMMENT "go build -> ${output_stem} (${PROJ_VERSION})"
        VERBATIM
    )
    if(all_target)
        add_custom_target(${target_name} ALL DEPENDS ${_output})
    else()
        add_custom_target(${target_name} DEPENDS ${_output})
    endif()
    if(target_name STREQUAL "goworker")
        set(GOWORKER_BIN ${_output} PARENT_SCOPE)
    endif()
endfunction()

# 旧 goworker target 保持原输出名与 GOWORKER_NO_PLUGINS 语义。
add_goworker_binary(goworker "goworker${_cross_suffix}" ON ${GO_OFF_TAGS})

# 显式 variant 输出互不覆盖；full 不受旧兼容参数影响。
set(_agent_tags "goworker_no_dispatcher")
set(_dispatcher_tags "goworker_no_agent")
set(_minimal_tags "goworker_no_agent" "goworker_no_dispatcher")
add_goworker_binary(goworker-full "goworker-full${_cross_suffix}" OFF)
add_goworker_binary(goworker-agent "goworker-agent${_cross_suffix}" OFF ${_agent_tags})
add_goworker_binary(goworker-dispatcher "goworker-dispatcher${_cross_suffix}" OFF ${_dispatcher_tags})
add_goworker_binary(goworker-minimal "goworker-minimal${_cross_suffix}" OFF ${_minimal_tags})

# ---- 辅助目标 ----
function(add_goworker_check_target target_name mode)
    set(_tags "${ARGN}")
    _go_tag_args("${_tags}" _tag_args)
    if(mode STREQUAL "test")
        set(_go_command test)
        set(_mode_args "")
    elseif(mode STREQUAL "race")
        set(_go_command test)
        set(_mode_args "-race")
    elseif(mode STREQUAL "vet")
        set(_go_command vet)
        set(_mode_args "")
    else()
        message(FATAL_ERROR "未知 goworker 检查类型：${mode}")
    endif()
    add_custom_target(${target_name}
        COMMAND ${GO_EXECUTABLE} ${_go_command} ${_mode_args} ${_tag_args} ${GO_TEST_PKGS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "go ${_go_command} ${mode} (${target_name})"
        VERBATIM
    )
endfunction()

# 旧检查 target 遵循 GOWORKER_NO_PLUGINS；新增 target 与显式产物一一对应。
add_goworker_check_target(test test ${GO_OFF_TAGS})
add_goworker_check_target(test-race race ${GO_OFF_TAGS})
add_goworker_check_target(vet vet ${GO_OFF_TAGS})
foreach(_variant IN ITEMS full agent dispatcher minimal)
    if(_variant STREQUAL "full")
        set(_variant_tags "")
    elseif(_variant STREQUAL "agent")
        set(_variant_tags "goworker_no_dispatcher")
    elseif(_variant STREQUAL "dispatcher")
        set(_variant_tags "goworker_no_agent")
    else()
        set(_variant_tags "goworker_no_agent" "goworker_no_dispatcher")
    endif()
    add_goworker_check_target("test-${_variant}" test ${_variant_tags})
    add_goworker_check_target("test-race-${_variant}" race ${_variant_tags})
    add_goworker_check_target("vet-${_variant}" vet ${_variant_tags})
endforeach()

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
