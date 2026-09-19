# mobile bind 构建指南（gomobile）

Android 前端只消费 `gomobile bind` 产物（AAR），本文记录本机验证过的构建环境与步骤。

## 已验证的兼容性

- Go **1.26.1** + gomobile/gobind（x/mobile `v0.0.0-20260908204917`）bind 通过（2026-09-19，hello-world 含 Go→Kotlin 接口回调）
- AAR 被 Gradle 8.9 / AGP 8.7.3 / minSdk 21 工程正常消费，`libgojni.so` 随 APK 打包

## 本机环境要求

```bash
export ANDROID_HOME=$HOME/Library/Android/sdk
export ANDROID_NDK_HOME=$ANDROID_HOME/ndk/23.1.7779620   # ndk-bundle(22.1) 亦可
export GOPROXY=https://goproxy.cn,direct                  # proxy.golang.org 直连超时
export PATH="$PATH:$(go env GOPATH)/bin"                  # gomobile 运行期要能找到 gobind
```

安装（首次）：

```bash
go install golang.org/x/mobile/cmd/gomobile@latest
go install golang.org/x/mobile/cmd/gobind@latest
gomobile init   # 同样需要上述环境变量
```

## 绑定包的约束

- bind 目标只能是非 `internal/` 的普通包（gobind 胶水在外部 module 生成，导入不了 internal 包）；已定在 `daemon/mobile`
- 被绑定的 module 必须显式依赖 x/mobile（Go 1.26 tool directive）：

  ```bash
  go get -tool golang.org/x/mobile/cmd/gobind@latest
  ```

- API 面：仅 string/bool/error/可绑定接口；无 map/func 字段/slice-of-struct/变参 —— 结构化数据一律 JSON 字符串

## 构建

```bash
# 在 daemon 模块目录（含 go.work 解析）
gomobile bind -target=android -androidapi 21 -javapkg=dev.tinguo.goworker \
  -o ../build/goworker.aar ./mobile

# 交付到 Android 工程
cp ../build/goworker.aar ~/AndroidStudioProjects/goworkerandroid/app/libs/goworker.aar
```

`-javapkg=dev.tinguo.goworker` 下，`daemon/mobile` 包的类生成在 `dev.tinguo.goworker.mobile.*`。

## Android 侧消费

`app/build.gradle.kts`：

```kotlin
implementation(fileTree(mapOf("dir" to "libs", "include" to listOf("*.aar"))))
```

线程模型：Go 回调可能来自任意 goroutine —— Kotlin 侧碰 UI 必须切主线程；`Run` 类阻塞调用放 `Dispatchers.IO`；Go panic 会 abort 进程，bind 面每个导出方法必须有 recover 壳。
