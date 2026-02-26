package core

import (
	"fmt"
)

// DefaultGapTolerance 是 Range 合并时的默认间隙容忍阈值（32KB）。
//
// CDN 对 Range 段数敏感（>30 段返回 400），但对数据量不敏感。
// 间隙 < 阈值时合并为同一 Range，多下载少量无用字节换取更少的段数。
const DefaultGapTolerance uint32 = 32 * 1024

// ScheduleConfig 是 Schedule 函数的配置参数。
type ScheduleConfig struct {
	MaxRangesPerReq int    // 单个 HTTP Range 请求的最大非连续段数（默认 30）
	GapTolerance    uint32 // 间隙容忍阈值（默认 32KB）
}

// Schedule 将 Mapper 的输出转换为可直接派发给 Worker 的 []BundleJob。
//
// 核心逻辑：
//  1. 对同一 BundleID 内已排序的 []GlobalChunkTask，执行连续段合并（含 Gap Tolerance）
//  2. 若单个 BundleJob 的 Range 数超过 maxRangesPerReq，分裂为多个 BundleJob
func Schedule(taskMap map[uint64][]GlobalChunkTask, cfg ScheduleConfig) []BundleJob {
	if cfg.MaxRangesPerReq <= 0 {
		cfg.MaxRangesPerReq = 30
	}

	jobs := make([]BundleJob, 0, len(taskMap))

	for bundleID, tasks := range taskMap {
		if len(tasks) == 0 {
			continue
		}

		ranges := mergeRanges(tasks, cfg.GapTolerance)

		bundleFilename := fmt.Sprintf("%016X.bundle", bundleID)
		splitJobs := splitBundleJob(bundleID, bundleFilename, ranges, cfg.MaxRangesPerReq)
		jobs = append(jobs, splitJobs...)
	}

	return jobs
}

// mergeRanges 将按 BundleOffset 升序排列的 GlobalChunkTask 合并为 ChunkRange 列表。
// gapTolerance=0 时退化为严格连续合并。
func mergeRanges(tasks []GlobalChunkTask, gapTolerance uint32) []ChunkRange {
	if len(tasks) == 0 {
		return nil
	}

	ranges := make([]ChunkRange, 0, len(tasks))

	current := ChunkRange{
		Start:  tasks[0].BundleOffset,
		End:    tasks[0].BundleOffset + tasks[0].CompressedSize - 1,
		Chunks: []GlobalChunkTask{tasks[0]},
	}

	for i := 1; i < len(tasks); i++ {
		task := tasks[i]
		taskStart := task.BundleOffset
		taskEnd := task.BundleOffset + task.CompressedSize - 1

		gap := taskStart - (current.End + 1)
		if gap <= gapTolerance {
			current.End = taskEnd
			current.Chunks = append(current.Chunks, task)
		} else {
			ranges = append(ranges, current)
			current = ChunkRange{
				Start:  taskStart,
				End:    taskEnd,
				Chunks: []GlobalChunkTask{task},
			}
		}
	}
	ranges = append(ranges, current)

	return ranges
}

func splitBundleJob(bundleID uint64, filename string, ranges []ChunkRange, maxRangesPerReq int) []BundleJob {
	if len(ranges) <= maxRangesPerReq {
		return []BundleJob{{
			BundleID:       bundleID,
			BundleFilename: filename,
			Ranges:         ranges,
		}}
	}

	jobs := make([]BundleJob, 0, (len(ranges)+maxRangesPerReq-1)/maxRangesPerReq)
	for i := 0; i < len(ranges); i += maxRangesPerReq {
		end := i + maxRangesPerReq
		if end > len(ranges) {
			end = len(ranges)
		}
		jobs = append(jobs, BundleJob{
			BundleID:       bundleID,
			BundleFilename: filename,
			Ranges:         ranges[i:end],
		})
	}
	return jobs
}
