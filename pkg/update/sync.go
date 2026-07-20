package update

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/Virace/RiotManifestGo/internal/fswriter"
	"github.com/Virace/RiotManifestGo/pkg/core"
	"github.com/Virace/RiotManifestGo/pkg/diff"
	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// moveCopyBufSize 是 Moved 文件整文件流式复制使用的缓冲区大小，避免大文件被
// 一次性读入内存。
const moveCopyBufSize = 1 << 20 // 1MB

// Mode 决定 Sync 如何确定需要处理哪些文件、以及是否信任本地已有数据。
type Mode int

const (
	// ModeAuto 默认模式：有旧清单时按 diff 做文件级跳过（Unchanged 直接跳过、不
	// 验证），Changed/Added 按 chunk 级校验补洞；无旧清单时退化为全部按 Changed
	// 处理（全量验证）。
	ModeAuto Mode = iota
	// ModeRepair 不做文件级跳过，逐文件按新清单重新验证补洞——用于修复被
	// ModeAuto 跳过掩盖的本地损坏（Spec §3 经验 1：AUTO 跳过的 Unchanged 文件
	// 从不验证，损坏无法自愈，必须有显式档位强制全量验证）。
	ModeRepair
	// ModeVerifyOnly 与 ModeRepair 一样逐文件验证，但零写盘、零下载、零存档，
	// 仅用于诊断本地完整性（dry-run）。
	ModeVerifyOnly
	// ModeForceFull 跳过验证，把全部文件当作全新内容整体下载（对应 CLI
	// -no-verify）。
	ModeForceFull
)

// Options 是 Sync 的行为参数。
type Options struct {
	Mode Mode
	// OldManifestPath 显式指定旧清单路径；为空时从 Archive.InstalledManifestPath()
	// 自动发现，仍找不到时视为无旧清单。ModeRepair/ModeVerifyOnly/ModeForceFull
	// 不使用旧清单，此字段被忽略。
	OldManifestPath string
	// RemoveDeleted 控制是否删除 diff 判定为 Removed 的旧路径；调用方需要
	// "默认删除"语义时应显式传 true（CLI -keep-removed 置 false）。只删除
	// diff.Result.Removed 列表内的路径，不做任何其他清理。
	RemoveDeleted bool
}

// Stats 汇总一次 Sync 执行的结果，供调用方输出统计与决定 exit code。
// 六个路径列表按处理结果互斥：一个文件最终只会出现在其中一个列表里。
type Stats struct {
	DownloadedBytes int64 // 网络补洞字节数（解压域），只统计最终成功提交的文件
	ReusedBytes     int64 // 本地复用字节数（PATCH 命中 + MOVE 复制），只统计最终成功提交的文件

	Skipped []string // 内容已确认完好、未做任何写入的文件（新路径）
	Patched []string // 本地已存在、按 chunk 级命中+下载补洞后提交的文件（新路径）
	Created []string // 本地不存在、整个从网络下载后提交的文件（新路径）
	Moved   []string // 从旧路径整文件复制并提交的文件（新路径）
	Removed []string // 已从磁盘删除的旧路径（仅 RemoveDeleted 时可能非空）
	// Failed 记录未能确认最终正确的文件：下载失败、Moved 源读取失败，或（仅
	// ModeVerifyOnly 下）验证发现本地缺失/损坏的 chunk——ModeVerifyOnly 本身
	// 零写盘，因此这里不代表下载失败，而是"需要一次真正的 Sync 才能修复"。
	Failed []string
}

// pendingFile 记录一个已经把内容写入 staging、等待统一提交/丢弃的目标文件。
type pendingFile struct {
	moved       bool   // true=Moved 整文件复制；false=Changed/Added/Repair/ForceFull 的 chunk 级 patch
	path        string // 清单相对路径（新路径）
	fullPath    string
	stagingPath string
	exists      bool  // patch 类：处理前磁盘上是否已存在同名文件（Created 与 Patched 的分类依据）
	reusedBytes int64 // 本地复用（读旧文件写 staging）的字节数
}

// Sync 是增量更新编排器：比较新旧清单、按 chunk 级复用本地数据、把缺失的 chunk
// 灌入 Downloader 完成网络补洞，最后统一提交并（成功时）推进 installed.json。
//
// files 是调用方传入的完整待处理文件集合（通常已经过 core.Filter），newManifest/
// rawManifest/source 只用于成功后的 Archive.Save（新清单存档）。
func Sync(ctx context.Context, newManifest *rman.Manifest, rawManifest []byte, source string,
	outputDir string, files []rman.FileEntry, d *core.Downloader, opts Options) (Stats, error) {

	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}

	diffResult, err := resolveDiff(outputDir, files, opts)
	if err != nil {
		return Stats{}, err
	}

	if opts.Mode == ModeVerifyOnly {
		return verifyOnlySync(ctx, diffResult.Changed, outputDir, d)
	}

	pool := fswriter.NewFilePool(0)
	defer pool.Close()

	stats := Stats{}
	pending := make([]pendingFile, 0, len(diffResult.Moved)+len(diffResult.Changed)+len(diffResult.Added))
	missesByPath := make(map[string][]ChunkRef)
	downloadedByPath := make(map[string]int64)
	failedSet := make(map[string]bool)

	// 步骤 3：Moved —— 旧路径整文件流式复制到新路径 staging，替换统一在步骤 6 做。
	for _, pair := range diffResult.Moved {
		if err := ctx.Err(); err != nil {
			return Stats{}, err
		}
		fullPath, err := core.OutputPath(outputDir, pair.Entry.Path)
		if err != nil {
			return Stats{}, err
		}
		srcPath, err := core.OutputPath(outputDir, pair.From)
		if err != nil {
			return Stats{}, err
		}
		pf, err := processMoved(pair.Entry.Path, fullPath, srcPath, pair.Entry.FileSize, pool, d)
		if err != nil {
			stats.Failed = append(stats.Failed, pair.Entry.Path)
			continue
		}
		pending = append(pending, pf)
	}

	// 步骤 4：Changed/Added（ModeRepair/ModeForceFull 下等价为全集）—— chunk 级
	// 验证补洞；ModeForceFull 跳过验证，全部视为 miss。
	toVerify := make([]rman.FileEntry, 0, len(diffResult.Changed)+len(diffResult.Added))
	toVerify = append(toVerify, diffResult.Changed...)
	toVerify = append(toVerify, diffResult.Added...)
	forceFull := opts.Mode == ModeForceFull

	for _, entry := range toVerify {
		if err := ctx.Err(); err != nil {
			return Stats{}, err
		}
		fullPath, err := core.OutputPath(outputDir, entry.Path)
		if err != nil {
			return Stats{}, err
		}
		pf, misses, skip, err := processPatchEntry(entry.Path, fullPath, entry, pool, d, forceFull)
		if err != nil {
			stats.Failed = append(stats.Failed, entry.Path)
			continue
		}
		if skip {
			stats.Skipped = append(stats.Skipped, entry.Path)
			d.Emit(core.EventFileSkipped{Path: fullPath})
			continue
		}
		if len(misses) > 0 {
			missesByPath[entry.Path] = misses
			var sum int64
			for _, m := range misses {
				sum += int64(m.Chunk.UncompressedSize)
			}
			downloadedByPath[entry.Path] = sum
		}
		pending = append(pending, pf)
	}

	// Unchanged：只有 ModeAuto 找到旧清单时才可能非空，文件级快速跳过，不做任何验证
	// （Spec §7 权衡：本地损坏的 Unchanged 文件在 AUTO 下不会被修复，需要 ModeRepair）。
	for _, entry := range diffResult.Unchanged {
		fullPath, err := core.OutputPath(outputDir, entry.Path)
		if err != nil {
			return Stats{}, err
		}
		stats.Skipped = append(stats.Skipped, entry.Path)
		d.Emit(core.EventFileSkipped{Path: fullPath})
	}

	// 步骤 5：Misses → taskMap → DownloadTasks(ManageStaging: false)，写入落 staging
	// 由 Downloader 保证；ManageStaging=false 下 DownloadTasks 返回前会释放它自己
	// 打开的 staging 句柄，本函数随后仍需对自己（Sync 侧 FilePool）持有的句柄
	// 单独 ClosePath（步骤 6）。
	if len(missesByPath) > 0 {
		taskMap := buildMissTaskMap(missesByPath)
		downloadFailed, dlErr := d.DownloadTasks(ctx, taskMap, core.TaskOptions{ManageStaging: false})
		if dlErr != nil && ctx.Err() != nil {
			// Context 取消：无法确认哪些 staging 已经完整，本函数尚未提交任何文件
			// （提交统一在步骤 6），直接返回，留待调用方决定是否重试整个 Sync。
			return Stats{}, ctx.Err()
		}
		for path := range downloadFailed {
			failedSet[path] = true
		}
	}

	// 步骤 6：提交/丢弃。非 failed 文件 ClosePath+CommitStaging（含 Moved，发
	// EventFileRenamed）；failed 文件 ClosePath+DiscardStaging。
	for _, pf := range pending {
		isFailed := failedSet[pf.path]
		closeErr := pool.ClosePath(pf.stagingPath)
		if isFailed || closeErr != nil {
			_ = fswriter.DiscardStaging(pf.fullPath)
			stats.Failed = append(stats.Failed, pf.path)
			continue
		}
		if err := fswriter.CommitStaging(pf.fullPath); err != nil {
			stats.Failed = append(stats.Failed, pf.path)
			continue
		}
		d.Emit(core.EventFileRenamed{From: pf.stagingPath, To: pf.fullPath})
		stats.ReusedBytes += pf.reusedBytes

		if pf.moved {
			stats.Moved = append(stats.Moved, pf.path)
			continue
		}
		stats.DownloadedBytes += downloadedByPath[pf.path]
		if pf.exists {
			stats.Patched = append(stats.Patched, pf.path)
		} else {
			stats.Created = append(stats.Created, pf.path)
		}
	}

	// 步骤 7：RemoveDeleted → 删除 Removed 路径（仅此列表），发 EventFileRemoved。
	if opts.RemoveDeleted {
		if err := removeDeleted(outputDir, diffResult.Removed, d, &stats); err != nil {
			return stats, err
		}
	}

	// 步骤 8：failed 为空 → Archive.Save(新清单)；否则不推进 installed.json。
	if len(stats.Failed) == 0 {
		archive := NewArchive(outputDir)
		if err := archive.Save(newManifest.ManifestID, rawManifest, source); err != nil {
			return stats, fmt.Errorf("存档新清单失败: %w", err)
		}
	}

	return stats, nil
}

// resolveDiff 按 opts.Mode 决定 diff.Result：
//   - ModeAuto：opts.OldManifestPath 优先，否则尝试 Archive 自动发现；找到则
//     rman.ParseFile + diff.Diff。显式指定的路径解析失败视为硬错误（用户明确
//     要求了某个旧清单，不能默默忽略）；自动发现的路径解析失败退化为"无旧清单"
//     （那只是内部存档缓存，不应让它的损坏拖垮整次 Sync）。完全没有旧清单时，
//     把全部新文件视为 Changed（Spec §7："Unchanged 默认不验证——不带旧清单跑
//     一次即得全量验证"），Unchanged/Added/Moved/Removed 均为空。
//   - ModeRepair/ModeVerifyOnly：不解析旧清单、不做 diff，全部视为 Changed
//     （Spec §3 经验 1：不做文件级跳过）。
//   - ModeForceFull：不解析旧清单，全部视为 Added（对应"跳过验证、整体重下"）。
func resolveDiff(outputDir string, files []rman.FileEntry, opts Options) (diff.Result, error) {
	switch opts.Mode {
	case ModeRepair, ModeVerifyOnly:
		return diff.Result{Changed: files}, nil
	case ModeForceFull:
		return diff.Result{Added: files}, nil
	}

	oldPath := opts.OldManifestPath
	explicit := oldPath != ""
	if !explicit {
		archive := NewArchive(outputDir)
		if p, ok := archive.InstalledManifestPath(); ok {
			oldPath = p
		}
	}
	if oldPath == "" {
		return diff.Result{Changed: files}, nil
	}

	oldManifest, err := rman.ParseFile(oldPath)
	if err != nil {
		if explicit {
			return diff.Result{}, fmt.Errorf("解析旧清单失败 %s: %w", oldPath, err)
		}
		return diff.Result{Changed: files}, nil
	}

	return diff.Diff(oldManifest.Files, files), nil
}

// verifyOnlySync 实现 ModeVerifyOnly：对 entries 逐一 VerifyFileChunks，零写盘、
// 零下载、零存档。Misses 为空的文件计入 Skipped；存在 Misses（本地缺失或损坏，
// 需要一次真正的 Sync 才能补齐）计入 Failed，语义详见 Stats.Failed 字段注释。
func verifyOnlySync(ctx context.Context, entries []rman.FileEntry, outputDir string, d *core.Downloader) (Stats, error) {
	stats := Stats{}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return Stats{}, err
		}
		fullPath, err := core.OutputPath(outputDir, entry.Path)
		if err != nil {
			return Stats{}, err
		}
		result, err := VerifyFileChunks(entry, fullPath)
		if err != nil {
			stats.Failed = append(stats.Failed, entry.Path)
			continue
		}
		if len(result.Misses) == 0 {
			stats.Skipped = append(stats.Skipped, entry.Path)
			d.Emit(core.EventFileSkipped{Path: fullPath})
			continue
		}
		stats.Failed = append(stats.Failed, entry.Path)
	}
	return stats, nil
}

// processMoved 把旧路径 srcPath 的整文件内容按流式方式复制到 fullPath 对应的
// staging：内容已由 diff 阶段的 ChunkID 序列相等性担保一致，因此不做逐 chunk
// 校验，只做一次顺序读写（而非一次性读入内存，兼容大文件）。
func processMoved(path, fullPath, srcPath string, fileSize uint64, pool *fswriter.FilePool, d *core.Downloader) (pendingFile, error) {
	stagingPath := fswriter.StagingPath(fullPath)
	if err := pool.PreallocateFile(stagingPath, int64(fileSize)); err != nil {
		return pendingFile{}, err
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return pendingFile{}, err
	}
	defer src.Close()

	var offset int64
	buf := make([]byte, moveCopyBufSize)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, writeErr := pool.WriteAt(stagingPath, buf[:n], offset); writeErr != nil {
				return pendingFile{}, writeErr
			}
			offset += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return pendingFile{}, readErr
		}
	}

	d.Emit(core.EventChunkReused{Path: fullPath, Bytes: offset})

	return pendingFile{
		moved:       true,
		path:        path,
		fullPath:    fullPath,
		stagingPath: stagingPath,
		reusedBytes: offset,
	}, nil
}

// processPatchEntry 处理一个 Changed/Added（或 ModeRepair/ModeForceFull 下的等价
// 全集）条目：ForceFull 跳过验证，直接把全部 Chunk 当作 miss；其余模式先
// VerifyFileChunks，Hits 从磁盘现有文件 ReadAt 后 WriteAt 进 staging（累计复用
// 字节，逐 chunk 发 EventChunkReused），Misses 返回给调用方汇总进下载任务集。
//
// 若磁盘现有文件已 100% 命中（Exists 且零 Miss），说明内容已经正确，不需要经过
// staging/rename——skip=true，调用方应归入 Skipped、不再触碰这个文件。这里额外
// 处理一个边界：现有文件长于清单声明的 FileSize 时（尾部残留数据），仍需
// Truncate 到位——"全部 chunk 命中"不等于"文件已经完整"。
func processPatchEntry(path, fullPath string, entry rman.FileEntry, pool *fswriter.FilePool, d *core.Downloader, forceFull bool) (pendingFile, []ChunkRef, bool, error) {
	if forceFull {
		stagingPath := fswriter.StagingPath(fullPath)
		if err := pool.PreallocateFile(stagingPath, int64(entry.FileSize)); err != nil {
			return pendingFile{}, nil, false, err
		}
		return pendingFile{
			path:        path,
			fullPath:    fullPath,
			stagingPath: stagingPath,
			exists:      false,
		}, allMisses(entry), false, nil
	}

	result, err := VerifyFileChunks(entry, fullPath)
	if err != nil {
		return pendingFile{}, nil, false, err
	}

	if result.Exists && len(result.Misses) == 0 {
		if info, statErr := os.Stat(fullPath); statErr == nil && uint64(info.Size()) > entry.FileSize {
			if truncErr := os.Truncate(fullPath, int64(entry.FileSize)); truncErr != nil {
				return pendingFile{}, nil, false, truncErr
			}
		}
		return pendingFile{}, nil, true, nil
	}

	stagingPath := fswriter.StagingPath(fullPath)
	if err := pool.PreallocateFile(stagingPath, int64(entry.FileSize)); err != nil {
		return pendingFile{}, nil, false, err
	}

	var reused int64
	if result.Exists && len(result.Hits) > 0 {
		old, err := os.Open(fullPath)
		if err != nil {
			return pendingFile{}, nil, false, err
		}
		for _, hit := range result.Hits {
			buf := make([]byte, hit.Chunk.UncompressedSize)
			if _, err := old.ReadAt(buf, hit.FileOffset); err != nil {
				old.Close()
				return pendingFile{}, nil, false, err
			}
			if _, err := pool.WriteAt(stagingPath, buf, hit.FileOffset); err != nil {
				old.Close()
				return pendingFile{}, nil, false, err
			}
			reused += int64(hit.Chunk.UncompressedSize)
			d.Emit(core.EventChunkReused{Path: fullPath, Bytes: int64(hit.Chunk.UncompressedSize)})
		}
		old.Close()
	}

	return pendingFile{
		path:        path,
		fullPath:    fullPath,
		stagingPath: stagingPath,
		exists:      result.Exists,
		reusedBytes: reused,
	}, result.Misses, false, nil
}

// buildMissTaskMap 把逐文件收集到的 Miss ChunkRef 转换为 DownloadTasks 需要的
// taskMap：按 ChunkID 全局去重（与 core.Map 同一不变式：相同 ChunkID 只出现一次，
// 多个 WriteTarget 合并），按 BundleID 分组并在组内按 BundleOffset 升序排列
// （Schedule 依赖这一顺序做连续段合并）。
//
// 不能直接复用 core.Map：core.Map 假定传入的是完整文件的完整 Chunks 序列，会按
// 传入顺序从 0 重新累加 FileOffset；这里必须原样使用 VerifyFileChunks 已经算好
// 的、相对完整新文件布局的偏移，否则下载回来的数据会被写到错误的位置。
func buildMissTaskMap(missesByPath map[string][]ChunkRef) map[uint64][]core.GlobalChunkTask {
	chunkIndex := make(map[uint64]*core.GlobalChunkTask)
	for path, misses := range missesByPath {
		for _, ref := range misses {
			target := core.WriteTarget{
				FilePath:    path,
				FileOffset:  ref.FileOffset,
				ExpectedLen: ref.Chunk.UncompressedSize,
				ChunkID:     ref.Chunk.ChunkID,
				HashType:    ref.Chunk.HashType,
			}
			if existing, ok := chunkIndex[ref.Chunk.ChunkID]; ok {
				existing.Targets = append(existing.Targets, target)
			} else {
				chunkIndex[ref.Chunk.ChunkID] = &core.GlobalChunkTask{
					BundleID:       ref.Chunk.BundleID,
					BundleOffset:   ref.Chunk.BundleOffset,
					CompressedSize: ref.Chunk.CompressedSize,
					Targets:        []core.WriteTarget{target},
				}
			}
		}
	}

	bundleMap := make(map[uint64][]core.GlobalChunkTask)
	for _, task := range chunkIndex {
		bundleMap[task.BundleID] = append(bundleMap[task.BundleID], *task)
	}
	for bundleID := range bundleMap {
		sort.Slice(bundleMap[bundleID], func(i, j int) bool {
			return bundleMap[bundleID][i].BundleOffset < bundleMap[bundleID][j].BundleOffset
		})
	}
	return bundleMap
}

// removeDeleted 删除 diff 判定为 Removed 的旧路径（仅这一列表内的路径，不做任何
// 其他清理）；文件已不存在视为删除成功（幂等）。单个文件的删除失败是尽力而为，
// 不阻塞其余文件、也不计入 Stats.Removed。
func removeDeleted(outputDir string, removed []string, d *core.Downloader, stats *Stats) error {
	for _, path := range removed {
		fullPath, err := core.OutputPath(outputDir, path)
		if err != nil {
			return err
		}
		if rmErr := os.Remove(fullPath); rmErr != nil && !os.IsNotExist(rmErr) {
			continue
		}
		stats.Removed = append(stats.Removed, path)
		d.Emit(core.EventFileRemoved{Path: fullPath})
	}
	return nil
}
