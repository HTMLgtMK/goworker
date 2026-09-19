#!/usr/bin/env bash
# 根据现有 tag 算出下一个 patch 版本号。
#
# 抽成独立脚本是为了能测——版本号算错会推一个错误的 tag 出去，
# 而 tag 删起来很烦（远端可能已经有人 fetch 过）。
#
# 用法: next-version.sh <latest-tag-or-empty>
# 输出: vX.Y.Z
#
# 注意：调用方负责用 `git tag -l 'v[0-9]*.[0-9]*.[0-9]*'` 把候选 tag 捞出来，
# 但那个 glob **拦不住预发布 tag**（`v1.2.4-rc1` 一样匹配 `[0-9]*`），
# 所以这里必须自己校验，不能假设传进来的就是干净的 vX.Y.Z。

set -euo pipefail

latest="${1:-}"

if [ -z "$latest" ]; then
    echo "v0.1.0"
    exit 0
fi

# 只接受严格的 vX.Y.Z。带预发布后缀（-rc1 / -2）的一律拒掉，
# 否则 `patch` 会变成 `4-rc1` 这种东西，进了 bash 算术就是两种灾难：
#   -rc1 -> bash 把 rc1 当变量名，set -u 下直接 unbound variable 崩掉
#   -2   -> `4-2` 求值成 2，算出 v1.2.3，版本号倒退还和已发布 tag 撞车
if [[ ! "$latest" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    # ${latest} 必须带花括号：后面紧跟全角括号，bash 会把它的首字节
    # 当成变量名的一部分，变成 `latest\xef: unbound variable` 这种鬼报错。
    echo "next-version: 无法解析的 tag: ${latest}（期望 vX.Y.Z）" >&2
    exit 1
fi

major="${BASH_REMATCH[1]}"
minor="${BASH_REMATCH[2]}"
patch="${BASH_REMATCH[3]}"

echo "v${major}.${minor}.$((patch + 1))"
