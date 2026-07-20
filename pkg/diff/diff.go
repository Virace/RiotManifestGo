// Package diff 基于 ChunkID 序列比较新旧两份清单（Manifest）的文件条目，
// 将文件分类为 未变化 / 已变化 / 新增 / 删除 / 移动，供上层增量更新编排使用。
//
// 判同规则（判同一律）：两个文件"内容相同"当且仅当其 Chunks 的 ChunkID 序列
// 逐元素相等（长度、顺序均需一致），与 Path、FileSize、Flags、Link 等其他字段无关。
// 本包只做纯计算，不涉及任何 I/O。
package diff

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// MovedPair 描述一次"移动"匹配：旧路径 From 与新清单条目 Entry 的 ChunkID 序列完全一致，
// 但 Path 发生了变化。
type MovedPair struct {
	From  string         // 旧路径
	Entry rman.FileEntry // 新清单条目
}

// Result 是 Diff 的分类结果，五个分类互斥。
//
// 顺序保证（均为稳定排序，与输入顺序一致，便于结果可复现）：
//   - Unchanged / Changed / Added 按其条目在 new 中的原始顺序排列；
//   - Removed 按其路径在 old 中的原始顺序排列；
//   - Moved 按 Entry 在 new 中的原始顺序排列。
type Result struct {
	Unchanged []rman.FileEntry // 新清单条目，Path+ChunkID 序列与旧完全一致
	Changed   []rman.FileEntry // Path 同、序列不同
	Added     []rman.FileEntry // 旧清单无此 Path（且未被 Moved 配对）
	Removed   []string         // 旧清单有、新清单无（且未被 Moved 配对）
	Moved     []MovedPair      // 序列一致但 Path 变化（一对一配对）
}

// sameChunks 判断两个 Chunk 序列是否完全一致：长度相同，且逐元素 ChunkID 相等。
func sameChunks(a, b []rman.ChunkInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ChunkID != b[i].ChunkID {
			return false
		}
	}
	return true
}

// fingerprint 计算 Chunk 序列的指纹，仅用于 Moved 候选的初步分桶，
// 避免 Removed 候选与 Added 候选之间做 O(n*m) 全量 sameChunks 比较。
//
// 指纹碰撞（不同序列得到相同指纹）是被允许的：分桶只是性能优化，
// 是否真正配对始终由 sameChunks 精确核对决定，碰撞不影响正确性。
func fingerprint(chunks []rman.ChunkInfo) uint32 {
	h := fnv.New32a()
	var buf [8]byte
	for _, c := range chunks {
		binary.LittleEndian.PutUint64(buf[:], c.ChunkID)
		h.Write(buf[:])
	}
	return h.Sum32()
}

// Diff 比较 old、new 两份清单的文件条目，返回分类结果。
//
// 处理分两轮：
//  1. 按 Path 精确匹配：Path 相同的条目直接归入 Unchanged（序列相同）或
//     Changed（序列不同），不参与后续 Moved 配对。
//  2. 对 Path 未匹配到的旧、新条目做 Moved 配对：以 ChunkID 序列指纹分桶，
//     指纹命中后用 sameChunks 精确核对；每个旧路径最多参与一次配对（一对一），
//     按 new 条目的原始顺序处理，先到先得。
//
// 配对失败的旧条目归入 Removed，配对失败的新条目归入 Added。
func Diff(old, new []rman.FileEntry) Result {
	oldByPath := make(map[string]rman.FileEntry, len(old))
	for _, e := range old {
		oldByPath[e.Path] = e
	}

	var result Result

	// matchedOldPaths 记录已通过第一轮 Path 精确匹配消费掉的旧路径，
	// 这些路径不再进入 Moved 配对候选池。
	matchedOldPaths := make(map[string]bool, len(old))

	// 第一轮：按 Path 精确匹配，划分 Unchanged / Changed。
	for _, ne := range new {
		oe, ok := oldByPath[ne.Path]
		if !ok {
			continue
		}
		matchedOldPaths[ne.Path] = true
		if sameChunks(oe.Chunks, ne.Chunks) {
			result.Unchanged = append(result.Unchanged, ne)
		} else {
			result.Changed = append(result.Changed, ne)
		}
	}

	// removedCandidates：Path 未能直接匹配的旧路径，按 old 原始顺序保留，
	// 是 Moved 配对与最终 Removed 判定的候选池。
	removedCandidates := make([]string, 0, len(old))
	for _, oe := range old {
		if !matchedOldPaths[oe.Path] {
			removedCandidates = append(removedCandidates, oe.Path)
		}
	}

	// 按指纹为 removed 候选分桶，桶内按 old 原始顺序保留 path，保证配对结果确定。
	buckets := make(map[uint32][]string, len(removedCandidates))
	for _, path := range removedCandidates {
		fp := fingerprint(oldByPath[path].Chunks)
		buckets[fp] = append(buckets[fp], path)
	}
	// paired 记录已被配对消费的旧路径，确保一对一。
	paired := make(map[string]bool, len(removedCandidates))

	// 第二轮：对 Path 未匹配的新条目尝试 Moved 配对。
	for _, ne := range new {
		if matchedOldPaths[ne.Path] {
			continue // 已在第一轮按 Path 处理
		}

		fp := fingerprint(ne.Chunks)
		var matchedFrom string
		found := false
		for _, candidate := range buckets[fp] {
			if paired[candidate] {
				continue // 该旧路径已被其他新条目配对，跳过
			}
			if sameChunks(oldByPath[candidate].Chunks, ne.Chunks) {
				matchedFrom = candidate
				found = true
				break
			}
		}

		if found {
			paired[matchedFrom] = true
			result.Moved = append(result.Moved, MovedPair{From: matchedFrom, Entry: ne})
		} else {
			result.Added = append(result.Added, ne)
		}
	}

	// 未被配对的 removed 候选即为真正的 Removed。
	for _, path := range removedCandidates {
		if !paired[path] {
			result.Removed = append(result.Removed, path)
		}
	}

	return result
}
