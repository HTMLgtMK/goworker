// Package mobile 是 Android（gobind）入口壳的占位：gomobile bind 的目标包，
// 与 cmd/goworker（CLI 壳）共享 internal/app 的同一套 Application 装配。
//
// 位置约束：不能放 cmd/ 下（gobind 绑不出 package main），也不能放
// internal/ 下（gobind 生成的胶水在外部 module 里，导入不了 internal 包），
// 只能是 module 根部的普通包。骨架（Callbacks/MobileTool/Host）随 Android
// 集成工作填充。
package mobile
