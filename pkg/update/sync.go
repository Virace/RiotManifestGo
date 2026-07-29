package update

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/Virace/RiotManifestGo/internal/fswriter"
	"github.com/Virace/RiotManifestGo/pkg/core"
	"github.com/Virace/RiotManifestGo/pkg/diff"
	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// moveCopyBufSize 是 Moved 文件整文件流式复制使用的缓冲区大小，避免大文件被
// 一次性读入内存。
const moveCopyBufSize = 1 << 20 // 1MB

// Operation 决定一次 Sync 是独立下载，还是维护带持久状态的受管理安装。
type Operation int

const (
	// OperationDownload 是默认操作：只处理当前 targets，不读取或写入 .rman，
	// 也没有移动复用和清理权限。
	OperationDownload Operation = iota
	// OperationInstall 显式启用受管理安装：读取 schema 2 覆盖、执行受授权的
	// 增量动作，并在整批成功后推进 installed.json。
	OperationInstall
)

// Mode 决定在选定 Operation 内如何校验和获取目标文件。
type Mode int

const (
	// ModeAuto 默认策略：独立下载逐文件校验补洞；受管理安装仅对已记录覆盖且
	// 通过普通文件/大小门禁的 Unchanged 文件做快速跳过。
	ModeAuto Mode = iota
	// ModeRepair 不做文件级跳过，逐文件按新清单重新验证补洞。
	ModeRepair
	// ModeVerifyOnly 与 ModeRepair 一样逐文件验证，但零写盘、零下载、零状态变更。
	ModeVerifyOnly
	// ModeForceFull 跳过验证，把全部文件当作全新内容整体下载（对应 CLI
	// -no-verify）。
	ModeForceFull
)

// Options 是 Sync 的行为参数。
type Options struct {
	Operation Operation
	Mode      Mode
	// OldManifestPath 仅供 OperationInstall 使用。显式旧清单只是 diff 提示；
	// 只有匹配的 schema 2 状态成员才能授权跳过、移动复用和清理。
	OldManifestPath string
	// RemoveDeleted 仅供 OperationInstall 使用，且只清理 schema 2 明确管理的
	// Removed 路径与成功 Moved 的旧路径。
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
	Removed []string // 已从磁盘删除的旧路径（仅 RemoveDeleted 时可能非空）；
	// 只对应 diff.Result.Removed 列表，成功 Moved 配对的旧路径清理不计入这里
	// （该迁移语义已由 EventFileRenamed 表达，见 Options.RemoveDeleted 注释）。
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

// syncPlan 保留本次目标动作，以及受信任旧覆盖到新状态的转换依据。
type syncPlan struct {
	result        diff.Result
	fullDiff      diff.Result
	previousState *InstalledState
	hasDiff       bool
	removedPaths  []string
	movedOldPaths []string
}

// Sync 编排独立下载或受管理安装：按 chunk 级复用本地数据、下载缺失内容并逐文件
// 原子提交。只有 OperationInstall 会读取/推进 installed.json 或执行清理。
//
// files 是本次调用实际要处理的文件子集（通常已经过 core.Filter，可能小于完整
// 新清单）。newManifest.Files 是完整新清单条目：受管理 ModeAuto 用它
// （而不是 files）与旧清单一起算完整 diff，再按 files 收窄到本次处理范围——
// 这样旧清单里过滤范围外、但完整新清单仍然声明的路径，既不会被误判为
// diff.Result.Removed（RemoveDeleted=true 时被误删），也不会被误配对成某个
// 过滤内 Added 条目的 Moved 源（同样导致误删）。rawManifest/source 只用于
// 受管理安装成功后的 Archive.Save，存档完整新清单，Files 记录实际确认的覆盖。
func Sync(ctx context.Context, newManifest *rman.Manifest, rawManifest []byte, source string,
	outputDir string, files []rman.FileEntry, d *core.Downloader, opts Options) (Stats, error) {

	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}

	plan, err := resolvePlan(outputDir, newManifest, files, opts)
	if err != nil {
		return Stats{}, err
	}
	diffResult := plan.result

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

	// Moved 阶段：旧路径整文件流式复制到新路径 staging，替换统一在提交阶段做。
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

	// 验证阶段：Changed/Added（ModeRepair/ModeForceFull 下等价为全集）—— chunk 级
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

	// Unchanged 只包含受管理 ModeAuto 下同时通过 state membership、普通文件和
	// 声明大小门禁的目标；门禁失败者已在 resolvePlan 中降级为 Changed。
	for _, entry := range diffResult.Unchanged {
		fullPath, err := core.OutputPath(outputDir, entry.Path)
		if err != nil {
			return Stats{}, err
		}
		stats.Skipped = append(stats.Skipped, entry.Path)
		d.Emit(core.EventFileSkipped{Path: fullPath})
	}

	// 下载阶段：Misses → taskMap → DownloadTasks(ManageStaging: false)，写入落 staging
	// 由 Downloader 保证；ManageStaging=false 下 DownloadTasks 返回前会释放它自己
	// 打开的 staging 句柄，本函数随后仍需在提交阶段对自己（Sync 侧 FilePool）持有
	// 的句柄单独 ClosePath。
	if len(missesByPath) > 0 {
		taskMap := buildMissTaskMap(missesByPath)
		downloadFailed, dlErr := d.DownloadTasks(ctx, taskMap, core.TaskOptions{ManageStaging: false})
		if dlErr != nil && ctx.Err() != nil {
			// Context 取消：无法确认哪些 staging 已经完整，本函数尚未提交任何文件
			// （提交统一在提交阶段），直接返回，留待调用方决定是否重试整个 Sync。
			return Stats{}, ctx.Err()
		}
		for path := range downloadFailed {
			failedSet[path] = true
		}
	}

	// 提交阶段：非 failed 文件 ClosePath+CommitStaging（含 Moved，发
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

	// 清理与状态推进构成一次受管理状态转换：任何目标失败时两者都不执行，避免
	// installed.json 仍指向旧状态、但它声明的旧文件已经被清掉。
	if opts.Operation == OperationInstall && len(stats.Failed) == 0 {
		if opts.RemoveDeleted {
			if err := removeMovedSources(outputDir, plan.movedOldPaths); err != nil {
				return stats, err
			}
			if err := removeDeleted(outputDir, plan.removedPaths, d, &stats); err != nil {
				return stats, err
			}
		}

		managedFiles := nextManagedFiles(outputDir, plan, stats)
		archive := NewArchive(outputDir)
		if err := archive.Save(newManifest.ManifestID, rawManifest, source, managedFiles); err != nil {
			return stats, fmt.Errorf("存档新清单失败: %w", err)
		}
	}

	return stats, nil
}

// resolvePlan 将 Operation 与 Mode 两个维度组合成实际动作。独立下载从不读取
// Archive；受管理安装可以用旧清单做完整 diff，但文件级快捷动作还必须由匹配的
// schema 2 Files 成员授权。
func resolvePlan(outputDir string, newManifest *rman.Manifest, files []rman.FileEntry, opts Options) (syncPlan, error) {
	if opts.Operation != OperationInstall || opts.Mode == ModeVerifyOnly {
		return syncPlan{result: nonDiffResult(files, opts.Mode)}, nil
	}

	archive := NewArchive(outputDir)
	state, err := archive.LoadInstalled()
	if err != nil {
		return syncPlan{}, err
	}

	oldManifest, err := selectOldManifest(archive, state, newManifest, opts.OldManifestPath)
	if err != nil {
		return syncPlan{}, err
	}
	if oldManifest == nil {
		return syncPlan{result: nonDiffResult(files, opts.Mode)}, nil
	}

	full := diff.Diff(oldManifest.Files, newManifest.Files)
	var trustedState *InstalledState
	if state != nil && state.Schema == schemaVersion &&
		state.ManifestID == fmt.Sprintf("%016X", oldManifest.ManifestID) {
		trustedState = state
	}
	removedPaths, movedOldPaths := authorizedCleanupPaths(full, files, trustedState)

	plan := syncPlan{
		result:        nonDiffResult(files, opts.Mode),
		fullDiff:      full,
		previousState: trustedState,
		hasDiff:       true,
		removedPaths:  removedPaths,
		movedOldPaths: movedOldPaths,
	}
	if opts.Mode == ModeAuto {
		plan.result = authorizeAutoActions(outputDir, filterDiffResult(full, files), trustedState)
	}
	return plan, nil
}

// selectOldManifest 让 schema 2 managed state 优先于 stale 显式 hint。state 已指向
// 当前新清单时可直接用 new；显式清单只有与 state ManifestID 匹配时才能替代
// state archive。schema 1 没有覆盖权限，仍保持显式 hint 优先。
func selectOldManifest(archive *Archive, state *InstalledState, newManifest *rman.Manifest,
	explicitPath string) (*rman.Manifest, error) {
	statePath, statePathOK := archive.InstalledManifestPath()
	if state != nil && state.Schema == schemaVersion && statePathOK {
		if state.ManifestID == fmt.Sprintf("%016X", newManifest.ManifestID) {
			return newManifest, nil
		}

		var explicitManifest *rman.Manifest
		var explicitErr error
		if explicitPath != "" {
			explicitManifest, explicitErr = rman.ParseFile(explicitPath)
			if explicitErr == nil &&
				state.ManifestID == fmt.Sprintf("%016X", explicitManifest.ManifestID) {
				return explicitManifest, nil
			}
		}

		stateManifest, stateErr := rman.ParseFile(statePath)
		if stateErr == nil {
			return stateManifest, nil
		}
		if explicitPath != "" {
			if explicitErr != nil {
				return nil, fmt.Errorf("解析旧清单失败 %s: %w", explicitPath, explicitErr)
			}
			return explicitManifest, nil
		}
		return nil, nil
	}

	oldPath := explicitPath
	explicit := oldPath != ""
	if !explicit && statePathOK {
		oldPath = statePath
	}
	if oldPath == "" {
		return nil, nil
	}
	oldManifest, err := rman.ParseFile(oldPath)
	if err != nil {
		if explicit {
			return nil, fmt.Errorf("解析旧清单失败 %s: %w", oldPath, err)
		}
		return nil, nil
	}
	return oldManifest, nil
}

func nonDiffResult(files []rman.FileEntry, mode Mode) diff.Result {
	if mode == ModeForceFull {
		return diff.Result{Added: files}
	}
	return diff.Result{Changed: files}
}

// filterDiffResult 把基于完整新旧清单算出的 full 差分结果，按 files（本次调用
// 实际要处理的子集，通常是 CLI -p/-f 过滤后的结果）收窄范围：
//   - Unchanged/Changed/Added：只保留 Path 命中 files 的条目。
//   - Moved：只保留 Entry.Path（新路径）命中 files 的配对；新路径不在本次处理
//     范围内的配对整体丢弃——既不复制内容，其旧路径也不会被当作"迁移源"清理。
//   - Removed：直接采用 full 的判定结果（旧清单声明过、完整新清单已不再声明），
//     额外防御性排除 files 命中的路径。正常情况下二者不会相交——一个 Path 不可能
//     既是"完整新清单已不存在"又同时出现在 files（files 的元素必然来自完整新
//     清单），这里的排除只是防御，不依赖它也能保证正确性。
func filterDiffResult(full diff.Result, files []rman.FileEntry) diff.Result {
	target := make(map[string]bool, len(files))
	for _, f := range files {
		target[f.Path] = true
	}

	var filtered diff.Result
	for _, e := range full.Unchanged {
		if target[e.Path] {
			filtered.Unchanged = append(filtered.Unchanged, e)
		}
	}
	for _, e := range full.Changed {
		if target[e.Path] {
			filtered.Changed = append(filtered.Changed, e)
		}
	}
	for _, e := range full.Added {
		if target[e.Path] {
			filtered.Added = append(filtered.Added, e)
		}
	}
	for _, pair := range full.Moved {
		if target[pair.Entry.Path] {
			filtered.Moved = append(filtered.Moved, pair)
		}
	}
	for _, path := range full.Removed {
		if !target[path] {
			filtered.Removed = append(filtered.Removed, path)
		}
	}
	return filtered
}

// authorizeAutoActions 将纯 manifest diff 收紧到 schema 2 的真实所有权边界。
// 未受管理或磁盘门禁失败的 Unchanged/Moved 目标都会退化为普通 PATCH。
func authorizeAutoActions(outputDir string, result diff.Result, state *InstalledState) diff.Result {
	managed := managedFileSet(state)
	authorized := diff.Result{
		Changed: append([]rman.FileEntry(nil), result.Changed...),
		Added:   append([]rman.FileEntry(nil), result.Added...),
	}

	for _, entry := range result.Unchanged {
		if _, ok := managed[managedPath(entry.Path)]; ok &&
			regularFileMatches(outputDir, entry.Path, entry.FileSize) {
			authorized.Unchanged = append(authorized.Unchanged, entry)
		} else {
			authorized.Changed = append(authorized.Changed, entry)
		}
	}
	for _, pair := range result.Moved {
		if _, ok := managed[managedPath(pair.From)]; ok &&
			regularFileMatches(outputDir, pair.From, pair.Entry.FileSize) {
			authorized.Moved = append(authorized.Moved, pair)
		} else {
			authorized.Changed = append(authorized.Changed, pair.Entry)
		}
	}
	return authorized
}

// authorizedCleanupPaths 独立于校验策略计算清理范围。真实 Removed 对所有 install
// 策略生效；Moved 旧源只有在对应新目标属于本轮选择时才纳入，确保 PATCH/REPAIR/
// FORCE_FULL 成功后也不会遗留受管理旧路径。
func authorizedCleanupPaths(full diff.Result, files []rman.FileEntry, state *InstalledState) ([]string, []string) {
	managed := managedFileSet(state)
	if len(managed) == 0 {
		return nil, nil
	}

	targets := make(map[string]struct{}, len(files))
	for _, entry := range files {
		targets[managedPath(entry.Path)] = struct{}{}
	}

	removed := make([]string, 0, len(full.Removed))
	for _, oldPath := range full.Removed {
		if _, ok := managed[managedPath(oldPath)]; ok {
			removed = append(removed, oldPath)
		}
	}

	movedOld := make([]string, 0, len(full.Moved))
	for _, pair := range full.Moved {
		_, selected := targets[managedPath(pair.Entry.Path)]
		_, owned := managed[managedPath(pair.From)]
		if selected && owned {
			movedOld = append(movedOld, pair.From)
		}
	}
	return removed, movedOld
}

func managedFileSet(state *InstalledState) map[string]struct{} {
	if state == nil || state.Schema != schemaVersion {
		return nil
	}
	files := make(map[string]struct{}, len(state.Files))
	for _, file := range state.Files {
		files[managedPath(file)] = struct{}{}
	}
	return files
}

func managedPath(file string) string {
	return strings.ReplaceAll(file, "\\", "/")
}

// regularFileMatches 是 AUTO 快速跳过、MOVE 源复用和跨版本覆盖携带共用的廉价
// 磁盘门禁。Lstat 明确拒绝符号链接；任何 stat/path 错误都按不匹配处理。
func regularFileMatches(outputDir, manifestPath string, size uint64) bool {
	fullPath, err := core.OutputPath(outputDir, manifestPath)
	if err != nil {
		return false
	}
	info, err := os.Lstat(fullPath)
	return err == nil && info.Mode().IsRegular() && uint64(info.Size()) == size
}

// nextManagedFiles 计算成功受管理安装后的 schema 2 覆盖。旧覆盖只携带在完整
// O→N diff 中内容未变且仍通过磁盘门禁的路径；本轮所有成功结果随后加入。
func nextManagedFiles(outputDir string, plan syncPlan, stats Stats) []string {
	files := make(map[string]struct{})
	if plan.hasDiff && plan.previousState != nil {
		previous := managedFileSet(plan.previousState)
		for _, entry := range plan.fullDiff.Unchanged {
			normalized := managedPath(entry.Path)
			if _, ok := previous[normalized]; ok &&
				regularFileMatches(outputDir, entry.Path, entry.FileSize) {
				files[normalized] = struct{}{}
			}
		}
	}

	for _, paths := range [][]string{
		stats.Skipped,
		stats.Patched,
		stats.Created,
		stats.Moved,
	} {
		for _, file := range paths {
			files[managedPath(file)] = struct{}{}
		}
	}

	result := make([]string, 0, len(files))
	for file := range files {
		result = append(result, file)
	}
	sort.Strings(result)
	return result
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

// discardPreallocatedStaging 尽力清理一个已经 PreallocateFile 成功、但调用方在
// 提交阶段之前就失败退出的 staging：先 ClosePath 释放 FilePool 持有的
// 句柄（Windows 下句柄未释放会导致后续删除失败），再删除 staging 文件本身。这两
// 步失败都不覆盖调用方已经拿到的真实错误，属于尽力而为的收尾。
func discardPreallocatedStaging(pool *fswriter.FilePool, fullPath, stagingPath string) {
	_ = pool.ClosePath(stagingPath)
	_ = fswriter.DiscardStaging(fullPath)
}

// processMoved 把旧路径 srcPath 的整文件内容按流式方式复制到 fullPath 对应的
// staging：内容已由 diff 阶段的 ChunkID 序列相等性担保一致，因此不做逐 chunk
// 校验，只做一次顺序读写（而非一次性读入内存，兼容大文件）。
//
// 预分配 staging 之后的任何错误退出（源文件打开/读取失败、写入 staging 失败）都
// 会先清理已经创建的 staging，不依赖调用方后续统一收尾——这类错误发生在文件进入
// 提交阶段之前，不清理就会残留半成品 staging。
func processMoved(path, fullPath, srcPath string, fileSize uint64, pool *fswriter.FilePool, d *core.Downloader) (pendingFile, error) {
	stagingPath := fswriter.StagingPath(fullPath)
	if err := pool.PreallocateFile(stagingPath, int64(fileSize)); err != nil {
		return pendingFile{}, err
	}

	src, err := os.Open(srcPath)
	if err != nil {
		discardPreallocatedStaging(pool, fullPath, stagingPath)
		return pendingFile{}, err
	}
	defer src.Close()

	var offset int64
	buf := make([]byte, moveCopyBufSize)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, writeErr := pool.WriteAt(stagingPath, buf[:n], offset); writeErr != nil {
				discardPreallocatedStaging(pool, fullPath, stagingPath)
				return pendingFile{}, writeErr
			}
			offset += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			discardPreallocatedStaging(pool, fullPath, stagingPath)
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

	// 以下 Hits 复制阶段的任何错误都会先清理刚预分配的 staging：这批错误发生在
	// 文件进入提交阶段之前，不清理就会残留半成品 staging。
	var reused int64
	if result.Exists && len(result.Hits) > 0 {
		old, err := os.Open(fullPath)
		if err != nil {
			discardPreallocatedStaging(pool, fullPath, stagingPath)
			return pendingFile{}, nil, false, err
		}
		for _, hit := range result.Hits {
			buf := make([]byte, hit.Chunk.UncompressedSize)
			if _, err := old.ReadAt(buf, hit.FileOffset); err != nil {
				old.Close()
				discardPreallocatedStaging(pool, fullPath, stagingPath)
				return pendingFile{}, nil, false, err
			}
			if _, err := pool.WriteAt(stagingPath, buf, hit.FileOffset); err != nil {
				old.Close()
				discardPreallocatedStaging(pool, fullPath, stagingPath)
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

// removeDeleted 删除已由 schema 2 ownership 筛选过的 Removed 路径。文件不存在
// 视为成功；其他删除错误会阻止状态推进，避免写出与磁盘清理结果不一致的新状态。
func removeDeleted(outputDir string, removed []string, d *core.Downloader, stats *Stats) error {
	for _, path := range removed {
		fullPath, err := core.OutputPath(outputDir, path)
		if err != nil {
			return err
		}
		info, statErr := os.Lstat(fullPath)
		if statErr != nil && !os.IsNotExist(statErr) {
			return fmt.Errorf("检查受管理旧文件 %s 失败: %w", path, statErr)
		}
		if statErr == nil && !info.Mode().IsRegular() {
			return fmt.Errorf("受管理旧路径不是普通文件: %s", path)
		}
		if rmErr := os.Remove(fullPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("删除受管理旧文件 %s 失败: %w", path, rmErr)
		}
		stats.Removed = append(stats.Removed, path)
		d.Emit(core.EventFileRemoved{Path: fullPath})
	}
	return nil
}

// removeMovedSources 删除本轮已成功建立新目标的受管理 Moved 旧路径。路径已不存在
// 视为成功；其他形态或删除错误会阻止状态推进。这里不写 Stats.Removed、不发事件。
func removeMovedSources(outputDir string, sources []string) error {
	for _, source := range sources {
		src, err := core.OutputPath(outputDir, source)
		if err != nil {
			return err
		}
		info, err := os.Lstat(src)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("检查受管理移动源 %s 失败: %w", src, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("受管理移动源不是普通文件: %s", src)
		}
		if err := os.Remove(src); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("删除受管理移动源 %s 失败: %w", src, err)
		}
	}
	return nil
}
