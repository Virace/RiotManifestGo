package fswriter

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestFilePool_PreallocateAndWrite 验证文件预分配 + WriteAt 基本流程。
func TestFilePool_PreallocateAndWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "test.bin")

	pool := NewFilePool(10)
	defer pool.Close()

	// 预分配 1024 字节
	if err := pool.PreallocateFile(path, 1024); err != nil {
		t.Fatalf("PreallocateFile 失败: %v", err)
	}

	// 验证文件大小
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("文件不存在: %v", err)
	}
	if info.Size() != 1024 {
		t.Errorf("文件大小 = %d, 期望 1024", info.Size())
	}

	// WriteAt 在偏移 100 写入数据
	data := []byte("hello world")
	n, err := pool.WriteAt(path, data, 100)
	if err != nil {
		t.Fatalf("WriteAt 失败: %v", err)
	}
	if n != len(data) {
		t.Errorf("写入字节数 = %d, 期望 %d", n, len(data))
	}

	// 读回验证
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}
	if !bytes.Equal(content[100:100+len(data)], data) {
		t.Errorf("读回的数据不匹配")
	}
}

// TestFilePool_LRUEviction 验证 LRU 淘汰机制。
func TestFilePool_LRUEviction(t *testing.T) {
	dir := t.TempDir()
	pool := NewFilePool(3) // 最多 3 个句柄
	defer pool.Close()

	// 创建 5 个文件
	for i := 0; i < 5; i++ {
		path := filepath.Join(dir, fileSuffix(i))
		pool.PreallocateFile(path, 100)
	}

	// 写入 5 个文件（超过池容量 3，触发 LRU 淘汰）
	for i := 0; i < 5; i++ {
		path := filepath.Join(dir, fileSuffix(i))
		_, err := pool.WriteAt(path, []byte{byte(i)}, 0)
		if err != nil {
			t.Fatalf("WriteAt file_%d 失败: %v", i, err)
		}
	}

	// 验证池中只有 3 个句柄
	pool.mu.Lock()
	handleCount := len(pool.handles)
	pool.mu.Unlock()

	if handleCount > 3 {
		t.Errorf("池中句柄数 = %d, 期望 ≤ 3", handleCount)
	}

	// 验证所有文件数据正确（淘汰后重新打开仍可写）
	for i := 0; i < 5; i++ {
		path := filepath.Join(dir, fileSuffix(i))
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("读取 file_%d 失败: %v", i, err)
		}
		if content[0] != byte(i) {
			t.Errorf("file_%d 数据不匹配: got %d, want %d", i, content[0], i)
		}
	}
}

// TestFilePool_ConcurrentWriteAt 验证多 goroutine 并发写入不同偏移的安全性。
func TestFilePool_ConcurrentWriteAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.bin")

	pool := NewFilePool(10)
	defer pool.Close()

	const fileSize = 1000
	pool.PreallocateFile(path, fileSize)

	// 10 个 goroutine 各写 100 字节，偏移不重叠
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			data := bytes.Repeat([]byte{byte(idx)}, 100)
			_, err := pool.WriteAt(path, data, int64(idx*100))
			errCh <- err
		}(i)
	}

	for i := 0; i < 10; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("并发 WriteAt 失败: %v", err)
		}
	}

	// 验证数据完整性
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			if content[i*100+j] != byte(i) {
				t.Errorf("偏移 %d 数据错误: got %d, want %d", i*100+j, content[i*100+j], i)
				break
			}
		}
	}
}

// TestFilePool_Close 验证 Close 后资源释放。
func TestFilePool_Close(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close_test.bin")

	pool := NewFilePool(10)
	pool.PreallocateFile(path, 100)
	pool.WriteAt(path, []byte("test"), 0)

	if err := pool.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	pool.mu.Lock()
	remaining := len(pool.handles)
	pool.mu.Unlock()

	if remaining != 0 {
		t.Errorf("Close 后池中仍有 %d 个句柄", remaining)
	}
}

func fileSuffix(i int) string {
	return filepath.Join("file_" + string(rune('0'+i)) + ".bin")
}
