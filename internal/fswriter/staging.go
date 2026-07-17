package fswriter

import (
	"fmt"
	"os"
)

// StagingSuffix 是暂存文件使用的后缀。
//
// 写入流程先落地到 <target>+StagingSuffix，写入并校验完成后通过 CommitStaging
// 原子替换为最终文件；写入失败或校验不通过则调用 DiscardStaging 清理暂存文件，
// 原目标文件保持不变。
const StagingSuffix = ".rman-tmp"

// StagingPath 返回 path 对应的暂存文件路径。
func StagingPath(path string) string {
	return path + StagingSuffix
}

// CommitStaging 将 path 对应的暂存文件原子替换为目标文件（os.Rename）。
//
// 调用前必须确保暂存文件与目标文件均无残留的打开句柄：Windows 下无法
// rename 覆盖仍被打开的文件，池化句柄需先调用 FilePool.ClosePath 关闭。
func CommitStaging(path string) error {
	staging := StagingPath(path)
	if err := os.Rename(staging, path); err != nil {
		return fmt.Errorf("提交暂存文件失败 %s -> %s: %w", staging, path, err)
	}
	return nil
}

// DiscardStaging 删除 path 对应的暂存文件；暂存文件不存在时视为成功。
func DiscardStaging(path string) error {
	staging := StagingPath(path)
	if err := os.Remove(staging); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("删除暂存文件失败 %s: %w", staging, err)
	}
	return nil
}
