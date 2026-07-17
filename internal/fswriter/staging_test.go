package fswriter

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCommitStagingReplacesExisting 验证 CommitStaging 会用暂存文件内容
// 原子替换已存在的目标文件（固化 Windows 下 os.Rename 覆盖已存在文件的语义）。
func TestCommitStagingReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target.bin")
	staging := StagingPath(path)

	if err := os.WriteFile(path, []byte("old content"), 0644); err != nil {
		t.Fatalf("写入旧目标文件失败: %v", err)
	}
	if err := os.WriteFile(staging, []byte("new content"), 0644); err != nil {
		t.Fatalf("写入暂存文件失败: %v", err)
	}

	if err := CommitStaging(path); err != nil {
		t.Fatalf("CommitStaging 失败: %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取目标文件失败: %v", err)
	}
	if string(content) != "new content" {
		t.Errorf("目标文件内容 = %q, 期望 %q", content, "new content")
	}

	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Errorf("暂存文件应已被 rename 移除，但 Stat 返回 err=%v", err)
	}
}

// TestDiscardStagingKeepsOriginal 验证 DiscardStaging 只删除暂存文件，
// 不影响已存在的目标文件；暂存文件本身不存在时也不应报错。
func TestDiscardStagingKeepsOriginal(t *testing.T) {
	t.Run("暂存文件存在", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "target.bin")
		staging := StagingPath(path)

		if err := os.WriteFile(path, []byte("original"), 0644); err != nil {
			t.Fatalf("写入目标文件失败: %v", err)
		}
		if err := os.WriteFile(staging, []byte("half-written"), 0644); err != nil {
			t.Fatalf("写入暂存文件失败: %v", err)
		}

		if err := DiscardStaging(path); err != nil {
			t.Fatalf("DiscardStaging 失败: %v", err)
		}

		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("读取目标文件失败: %v", err)
		}
		if string(content) != "original" {
			t.Errorf("目标文件内容 = %q, 期望保持 %q", content, "original")
		}

		if _, err := os.Stat(staging); !os.IsNotExist(err) {
			t.Errorf("暂存文件应已被删除，但 Stat 返回 err=%v", err)
		}
	})

	t.Run("暂存文件不存在", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "target.bin")

		if err := os.WriteFile(path, []byte("original"), 0644); err != nil {
			t.Fatalf("写入目标文件失败: %v", err)
		}

		if err := DiscardStaging(path); err != nil {
			t.Fatalf("暂存文件不存在时 DiscardStaging 不应报错: %v", err)
		}
	})
}

// TestClosePathThenRename 验证池中仍持有暂存文件打开句柄时直接 Commit 会失败，
// 必须先调用 ClosePath 关闭句柄后 CommitStaging 才能成功；
// 这固化了 Windows 下无法 rename 覆盖持有打开句柄文件的限制。
func TestClosePathThenRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target.bin")
	staging := StagingPath(path)

	if err := os.WriteFile(path, []byte("old content"), 0644); err != nil {
		t.Fatalf("写入旧目标文件失败: %v", err)
	}

	pool := NewFilePool(10)
	defer pool.Close()

	newContent := []byte("new content")
	if err := pool.PreallocateFile(staging, int64(len(newContent))); err != nil {
		t.Fatalf("PreallocateFile 失败: %v", err)
	}
	if _, err := pool.WriteAt(staging, newContent, 0); err != nil {
		t.Fatalf("WriteAt 失败: %v", err)
	}

	// 池中仍持有 staging 的打开句柄，此时直接 Commit 预期失败。
	if err := CommitStaging(path); err == nil {
		t.Fatalf("句柄仍打开时 CommitStaging 预期失败，但成功了")
	}

	if err := pool.ClosePath(staging); err != nil {
		t.Fatalf("ClosePath 失败: %v", err)
	}

	if err := CommitStaging(path); err != nil {
		t.Fatalf("ClosePath 后 CommitStaging 仍失败: %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取目标文件失败: %v", err)
	}
	if string(content) != string(newContent) {
		t.Errorf("目标文件内容 = %q, 期望 %q", content, newContent)
	}

	// 对未知路径调用 ClosePath 应视为已关闭，不报错。
	if err := pool.ClosePath(filepath.Join(dir, "unknown.bin")); err != nil {
		t.Errorf("对未知路径调用 ClosePath 不应报错: %v", err)
	}
}
