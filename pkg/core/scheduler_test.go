package core

import (
	"testing"
)

// ---- Scheduler 测试 ----

// TestSchedule_EmptyInput 验证空输入返回空 Job 列表。
func TestSchedule_EmptyInput(t *testing.T) {
	jobs := Schedule(nil, ScheduleConfig{MaxRangesPerReq: 30})
	if len(jobs) != 0 {
		t.Errorf("空输入期望 0 个 Job，得到 %d", len(jobs))
	}
}

// TestSchedule_ContiguousMerge 验证连续 Chunk 被合并到同一个 ChunkRange。
func TestSchedule_ContiguousMerge(t *testing.T) {
	const bundleID uint64 = 0xAA00000000000001

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "a.bin", FileOffset: 0, ExpectedLen: 200, ChunkID: 0x01}}},
			{BundleID: bundleID, BundleOffset: 100, CompressedSize: 150,
				Targets: []WriteTarget{{FilePath: "a.bin", FileOffset: 200, ExpectedLen: 300, ChunkID: 0x02}}},
			{BundleID: bundleID, BundleOffset: 250, CompressedSize: 200,
				Targets: []WriteTarget{{FilePath: "a.bin", FileOffset: 500, ExpectedLen: 400, ChunkID: 0x03}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{MaxRangesPerReq: 30})

	if len(jobs) != 1 {
		t.Fatalf("期望 1 个 BundleJob，得到 %d", len(jobs))
	}

	job := jobs[0]
	// 3 个连续 Chunk 应合并为 1 个 Range
	if len(job.Ranges) != 1 {
		t.Fatalf("3 个连续 Chunk 期望合并为 1 个 Range，得到 %d 个 Range", len(job.Ranges))
	}

	r := job.Ranges[0]
	if r.Start != 0 {
		t.Errorf("Range.Start = %d, 期望 0", r.Start)
	}
	// End = 0+100+150+200 - 1 = 449
	if r.End != 449 {
		t.Errorf("Range.End = %d, 期望 449", r.End)
	}
	if len(r.Chunks) != 3 {
		t.Errorf("Range 内 Chunk 数 = %d, 期望 3", len(r.Chunks))
	}
}

// TestSchedule_NonContiguous 验证非连续 Chunk 生成独立 Range。
func TestSchedule_NonContiguous(t *testing.T) {
	const bundleID uint64 = 0xBB00000000000001

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
			// 偏移 200，不紧邻 100（gap at 100-199）
			{BundleID: bundleID, BundleOffset: 200, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x02}}},
			// 偏移 300，紧邻 200+100=300
			{BundleID: bundleID, BundleOffset: 300, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x03}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{MaxRangesPerReq: 30})
	if len(jobs) != 1 {
		t.Fatalf("期望 1 个 BundleJob，得到 %d", len(jobs))
	}

	// 应有 2 个 Range：[0-99] 和 [200-399]
	ranges := jobs[0].Ranges
	if len(ranges) != 2 {
		t.Fatalf("期望 2 个 Range（1个gap），得到 %d", len(ranges))
	}

	// Range 0: 单 Chunk [0-99]
	if ranges[0].Start != 0 || ranges[0].End != 99 {
		t.Errorf("Range[0] = [%d-%d], 期望 [0-99]", ranges[0].Start, ranges[0].End)
	}

	// Range 1: 2 个连续 Chunk [200-399]
	if ranges[1].Start != 200 || ranges[1].End != 399 {
		t.Errorf("Range[1] = [%d-%d], 期望 [200-399]", ranges[1].Start, ranges[1].End)
	}
	if len(ranges[1].Chunks) != 2 {
		t.Errorf("Range[1] 内 Chunk 数 = %d, 期望 2", len(ranges[1].Chunks))
	}
}

// TestSchedule_SplitExceedingRanges 验证超过 maxRangesPerReq 时分裂 BundleJob。
//
// CDN 限制单请求最多约 30 个非连续 Range。
func TestSchedule_SplitExceedingRanges(t *testing.T) {
	const bundleID uint64 = 0xCC00000000000001

	// 构造 35 个非连续 Chunk（每个间隔 100 字节的 gap）
	tasks := make([]GlobalChunkTask, 35)
	for i := 0; i < 35; i++ {
		offset := uint32(i * 200) // 每个 Chunk size=100，间隔 100
		tasks[i] = GlobalChunkTask{
			BundleID:       bundleID,
			BundleOffset:   offset,
			CompressedSize: 100,
			Targets:        []WriteTarget{{FilePath: "x.bin", ChunkID: uint64(i)}},
		}
	}

	taskMap := map[uint64][]GlobalChunkTask{bundleID: tasks}
	jobs := Schedule(taskMap, ScheduleConfig{MaxRangesPerReq: 30})

	// 35 个非连续 Range 应被分为 2 个 BundleJob (30 + 5)
	if len(jobs) != 2 {
		t.Fatalf("35 个非连续 Range 期望分为 2 个 BundleJob，得到 %d", len(jobs))
	}

	// 第一个 Job 应有 30 个 Range
	if len(jobs[0].Ranges) != 30 {
		t.Errorf("Job[0] Range 数 = %d, 期望 30", len(jobs[0].Ranges))
	}

	// 第二个 Job 应有 5 个 Range
	if len(jobs[1].Ranges) != 5 {
		t.Errorf("Job[1] Range 数 = %d, 期望 5", len(jobs[1].Ranges))
	}

	// 两个 Job 的 BundleFilename 应相同
	if jobs[0].BundleFilename != jobs[1].BundleFilename {
		t.Errorf("分裂后的 BundleFilename 不一致: %q vs %q",
			jobs[0].BundleFilename, jobs[1].BundleFilename)
	}
}

// TestSchedule_BundleFilename 验证 BundleFilename 的格式正确（%016X.bundle）。
func TestSchedule_BundleFilename(t *testing.T) {
	const bundleID uint64 = 0x1A2B3C4D5E6F0000

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{MaxRangesPerReq: 30})
	if len(jobs) == 0 {
		t.Fatal("期望至少 1 个 Job")
	}

	want := "1A2B3C4D5E6F0000.bundle"
	if jobs[0].BundleFilename != want {
		t.Errorf("BundleFilename = %q, 期望 %q", jobs[0].BundleFilename, want)
	}
}

// TestSchedule_MultipleBundles 验证多个 BundleID 分别生成独立 Job。
func TestSchedule_MultipleBundles(t *testing.T) {
	const (
		bundle1 uint64 = 0x0000000000000001
		bundle2 uint64 = 0x0000000000000002
	)

	taskMap := map[uint64][]GlobalChunkTask{
		bundle1: {
			{BundleID: bundle1, BundleOffset: 0, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
		},
		bundle2: {
			{BundleID: bundle2, BundleOffset: 0, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "b.bin", ChunkID: 0x02}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{MaxRangesPerReq: 30})
	if len(jobs) != 2 {
		t.Fatalf("2 个不同 BundleID 期望 2 个 Job，得到 %d", len(jobs))
	}
}

// ---- 整包覆盖率判定测试 ----

// TestSchedule_FullBundle_CoverageFull 验证覆盖率达到 1.0 时触发整包作业。
func TestSchedule_FullBundle_CoverageFull(t *testing.T) {
	const bundleID uint64 = 0xD100000000000001

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 1000,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq:     30,
		FullBundleThreshold: 0.7,
		BundleSizes:         map[uint64]uint32{bundleID: 1000},
	})

	if len(jobs) != 1 {
		t.Fatalf("期望 1 个 BundleJob，得到 %d", len(jobs))
	}
	if !jobs[0].FullBundle {
		t.Errorf("覆盖率 1.0 期望 FullBundle=true")
	}
	if jobs[0].BundleSize != 1000 {
		t.Errorf("BundleSize = %d, 期望 1000", jobs[0].BundleSize)
	}
	// 整包作业仍保留 Ranges，供下游写盘定位 Chunk 目标。
	if len(jobs[0].Ranges) != 1 {
		t.Errorf("整包作业 Ranges 数 = %d, 期望 1", len(jobs[0].Ranges))
	}
}

// TestSchedule_FullBundle_ExactlyAtThreshold 验证覆盖率恰好等于阈值时触发（边界，>=）。
func TestSchedule_FullBundle_ExactlyAtThreshold(t *testing.T) {
	const bundleID uint64 = 0xD100000000000002

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			// 覆盖 700 字节，总大小 1000，覆盖率恰好 0.7
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 700,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq:     30,
		FullBundleThreshold: 0.7,
		BundleSizes:         map[uint64]uint32{bundleID: 1000},
	})

	if len(jobs) != 1 {
		t.Fatalf("期望 1 个 BundleJob，得到 %d", len(jobs))
	}
	if !jobs[0].FullBundle {
		t.Errorf("覆盖率恰好等于阈值（0.7）期望 FullBundle=true")
	}
}

// TestSchedule_FullBundle_BelowThreshold 验证覆盖率低于阈值时不触发整包作业。
func TestSchedule_FullBundle_BelowThreshold(t *testing.T) {
	const bundleID uint64 = 0xD100000000000003

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			// 覆盖 699 字节，总大小 1000，覆盖率 0.699 < 0.7
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 699,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq:     30,
		FullBundleThreshold: 0.7,
		BundleSizes:         map[uint64]uint32{bundleID: 1000},
	})

	if len(jobs) != 1 {
		t.Fatalf("期望 1 个 BundleJob，得到 %d", len(jobs))
	}
	if jobs[0].FullBundle {
		t.Errorf("覆盖率低于阈值期望 FullBundle=false")
	}
	if jobs[0].BundleSize != 0 {
		t.Errorf("非整包作业 BundleSize 期望 0，得到 %d", jobs[0].BundleSize)
	}
}

// TestSchedule_FullBundle_ThresholdDisabledByDefault 验证阈值为零值（默认）时，
// 即便覆盖率达到 1.0 也不触发整包作业——默认零行为变化。
func TestSchedule_FullBundle_ThresholdDisabledByDefault(t *testing.T) {
	const bundleID uint64 = 0xD100000000000004

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 1000,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq: 30,
		// FullBundleThreshold 未设置，零值
		BundleSizes: map[uint64]uint32{bundleID: 1000},
	})

	if len(jobs) != 1 {
		t.Fatalf("期望 1 个 BundleJob，得到 %d", len(jobs))
	}
	if jobs[0].FullBundle {
		t.Errorf("FullBundleThreshold 为零值时期望 FullBundle=false（默认禁用）")
	}
}

// TestSchedule_FullBundle_MissingBundleSize 验证 BundleSizes 缺该 Bundle 条目时不触发整包作业。
func TestSchedule_FullBundle_MissingBundleSize(t *testing.T) {
	const bundleID uint64 = 0xD100000000000005

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 1000,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq:     30,
		FullBundleThreshold: 0.7,
		BundleSizes:         map[uint64]uint32{}, // 不含 bundleID
	})

	if len(jobs) != 1 {
		t.Fatalf("期望 1 个 BundleJob，得到 %d", len(jobs))
	}
	if jobs[0].FullBundle {
		t.Errorf("BundleSizes 缺该 Bundle 条目时期望 FullBundle=false")
	}
}

// TestSchedule_FullBundle_ExceedsMaxRanges 验证整包作业不受 MaxRangesPerReq 拆分限制：
// 即便合并后的 Range 数超过上限，整包作业仍是单个 BundleJob。
func TestSchedule_FullBundle_ExceedsMaxRanges(t *testing.T) {
	const bundleID uint64 = 0xD100000000000006

	// 构造 35 个非连续 Chunk（每个间隔 100 字节的 gap），总覆盖 35*100=3500 字节
	tasks := make([]GlobalChunkTask, 35)
	for i := 0; i < 35; i++ {
		offset := uint32(i * 200) // 每个 Chunk size=100，间隔 100
		tasks[i] = GlobalChunkTask{
			BundleID:       bundleID,
			BundleOffset:   offset,
			CompressedSize: 100,
			Targets:        []WriteTarget{{FilePath: "x.bin", ChunkID: uint64(i)}},
		}
	}

	taskMap := map[uint64][]GlobalChunkTask{bundleID: tasks}
	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq:     30,
		FullBundleThreshold: 0.7,
		BundleSizes:         map[uint64]uint32{bundleID: 3500},
	})

	if len(jobs) != 1 {
		t.Fatalf("整包作业期望不拆分，得到 %d 个 BundleJob", len(jobs))
	}
	if !jobs[0].FullBundle {
		t.Errorf("覆盖率 1.0 期望 FullBundle=true")
	}
	if len(jobs[0].Ranges) != 35 {
		t.Errorf("整包作业 Ranges 数 = %d, 期望 35（不因超过 MaxRangesPerReq 而拆分）", len(jobs[0].Ranges))
	}
}

// TestSchedule_FullBundle_GapCountsTowardCoverage 验证 Gap 合并产生的间隙字节计入覆盖率分子。
//
// 两个 Chunk 各 100 字节，中间有 40 字节 Gap（GapTolerance=40 触发合并），
// 合并后的 Range 跨度为 240 字节，大于两个 Chunk 实际数据量之和（200 字节）。
// 若覆盖率分子错误地只统计 Chunk 数据量（200/240=0.833<0.9），不会触发；
// 只有正确统计合并后 Range 跨度（240/240=1.0>=0.9）才会触发。
func TestSchedule_FullBundle_GapCountsTowardCoverage(t *testing.T) {
	const bundleID uint64 = 0xD100000000000007

	taskMap := map[uint64][]GlobalChunkTask{
		bundleID: {
			{BundleID: bundleID, BundleOffset: 0, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x01}}},
			// gap = 140 - (99+1) = 40，等于 GapTolerance，触发合并
			{BundleID: bundleID, BundleOffset: 140, CompressedSize: 100,
				Targets: []WriteTarget{{FilePath: "a.bin", ChunkID: 0x02}}},
		},
	}

	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq:     30,
		GapTolerance:        40,
		FullBundleThreshold: 0.9,
		BundleSizes:         map[uint64]uint32{bundleID: 240},
	})

	if len(jobs) != 1 {
		t.Fatalf("期望 1 个 BundleJob，得到 %d", len(jobs))
	}
	if len(jobs[0].Ranges) != 1 {
		t.Fatalf("期望 Gap 合并为 1 个 Range，得到 %d", len(jobs[0].Ranges))
	}
	if !jobs[0].FullBundle {
		t.Errorf("Gap 合并跨度应计入覆盖率分子（240/240=1.0>=0.9），期望 FullBundle=true")
	}
}
