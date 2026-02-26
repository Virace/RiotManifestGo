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
