// Package fswriter 提供并发安全的 LRU 文件句柄池与 WriteAt 封装。
//
// 多 Worker 并发写数千个文件时，OS 文件描述符会耗尽。
// 本包通过 LRU 池化句柄，限制同时打开的文件数量。
//
// 线程安全：所有公开方法使用 sync.Mutex 保护。
// WriteAt 使用 pwrite 语义（Go 的 *os.File.WriteAt），不同 offset 天然并发安全。
package fswriter

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FilePool 是一个 LRU 文件句柄池。
//
//   - 上限 maxHandles 个 *os.File 句柄
//   - Get(path) 取句柄，不存在则 Open 并放入池
//   - 池满时淘汰最久未访问的句柄并 Close
type FilePool struct {
	mu         sync.Mutex
	maxHandles int
	handles    map[string]*list.Element
	lru        *list.List
}

type poolEntry struct {
	path    string
	file    *os.File
	refs    int
	evicted bool
}

// NewFilePool 创建一个新的 LRU 文件句柄池。
func NewFilePool(maxHandles int) *FilePool {
	if maxHandles <= 0 {
		maxHandles = 500
	}
	return &FilePool{
		maxHandles: maxHandles,
		handles:    make(map[string]*list.Element, maxHandles),
		lru:        list.New(),
	}
}

// WriteAt 将 data 写入指定文件的指定偏移（pwrite 语义，不同 offset 天然并发安全）。
func (p *FilePool) WriteAt(path string, data []byte, offset int64) (int, error) {
	entry, err := p.get(path)
	if err != nil {
		return 0, err
	}
	defer p.release(entry)
	return entry.file.WriteAt(data, offset)
}

func (p *FilePool) release(entry *poolEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && entry.evicted {
		entry.file.Close()
	}
}

// PreallocateFile 预分配文件空间：创建父目录 + Truncate 到指定大小。
func (p *FilePool) PreallocateFile(path string, size int64) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建目录失败 %s: %w", dir, err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("创建文件失败 %s: %w", path, err)
	}

	if err := f.Truncate(size); err != nil {
		f.Close()
		return fmt.Errorf("预分配文件空间失败 %s (%d bytes): %w", path, size, err)
	}
	f.Close()
	return nil
}

// Close 关闭池中所有打开的文件句柄。
func (p *FilePool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for _, elem := range p.handles {
		entry := elem.Value.(*poolEntry)
		entry.evicted = true
		if entry.refs == 0 {
			if err := entry.file.Close(); err != nil {
				lastErr = err
			}
		}
	}
	p.handles = make(map[string]*list.Element)
	p.lru.Init()
	return lastErr
}

func (p *FilePool) get(path string) (*poolEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if elem, ok := p.handles[path]; ok {
		p.lru.MoveToFront(elem)
		entry := elem.Value.(*poolEntry)
		entry.refs++
		return entry, nil
	}

	for p.lru.Len() >= p.maxHandles {
		p.evict()
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败 %s: %w", path, err)
	}

	entry := &poolEntry{path: path, file: f, refs: 1}
	elem := p.lru.PushFront(entry)
	p.handles[path] = elem
	return entry, nil
}

func (p *FilePool) evict() {
	back := p.lru.Back()
	if back == nil {
		return
	}
	entry := p.lru.Remove(back).(*poolEntry)
	delete(p.handles, entry.path)
	entry.evicted = true
	if entry.refs == 0 {
		entry.file.Close()
	}
}
