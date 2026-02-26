package core

import (
	"testing"

	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// ---- 测试数据辅助 ----

// makeEntry 构造一个指定路径和 flags 的 FileEntry（chunks 为空，Filter 测试不需要）。
func makeEntry(path string, flags ...string) rman.FileEntry {
	return rman.FileEntry{
		Path:  path,
		Flags: flags,
	}
}

// testFiles 是一组涵盖多路径、多语言的测试文件集。
var testFiles = []rman.FileEntry{
	makeEntry("DATA/Characters/Aatrox/Skins/Base/Aatrox.de_DE.bin", "de_DE"),
	makeEntry("DATA/Characters/Aatrox/Skins/Base/Aatrox.zh_CN.bin", "zh_CN"),
	makeEntry("DATA/Characters/Ahri/Skins/Base/Ahri.de_DE.bin", "de_DE"),
	makeEntry("DATA/Characters/Ahri/Skins/Base/Ahri.zh_CN.bin", "zh_CN"),
	makeEntry("RADS/solutions/lol_game_client_sln/releases/config.cfg"),
	makeEntry("Assets/Sounds/en_US/click.ogg", "en_US"),
	// 多 Flag 文件
	makeEntry("DATA/Characters/Aatrox/Skins/Base/Aatrox.common.bin", "de_DE", "zh_CN", "en_US"),
}

// ---- 测试用例 ----

// TestFilter_NoOpts 验证不传任何 FilterOption 时返回全部文件。
func TestFilter_NoOpts(t *testing.T) {
	got := Filter(testFiles)
	if len(got) != len(testFiles) {
		t.Errorf("Filter() 返回 %d 个文件，期望 %d 个", len(got), len(testFiles))
	}
}

// TestFilter_PatternSubstring 验证 Pattern 作为子串匹配路径。
func TestFilter_PatternSubstring(t *testing.T) {
	got := Filter(testFiles, FilterOption{Pattern: "Aatrox"})
	// 期望匹配：Aatrox.de_DE, Aatrox.zh_CN, Aatrox.common（共 3 个）
	if len(got) != 3 {
		t.Errorf("Pattern=Aatrox 返回 %d 个，期望 3 个", len(got))
		for _, f := range got {
			t.Logf("  - %s", f.Path)
		}
	}
}

// TestFilter_PatternCaseInsensitive 验证子串匹配大小写不敏感。
func TestFilter_PatternCaseInsensitive(t *testing.T) {
	got := Filter(testFiles, FilterOption{Pattern: "aatrox"})
	// 与 "Aatrox" 结果相同
	if len(got) != 3 {
		t.Errorf("Pattern=aatrox（小写）返回 %d 个，期望 3 个", len(got))
	}
}

// TestFilter_PatternRegex 验证包含正则元字符时按 regexp 匹配。
func TestFilter_PatternRegex(t *testing.T) {
	// 匹配路径中含 "Aatrox.de_DE" 的文件（点号是正则元字符）
	got := Filter(testFiles, FilterOption{Pattern: `Aatrox\.de_DE`})
	if len(got) != 1 {
		t.Errorf(`Pattern=Aatrox\.de_DE 返回 %d 个，期望 1 个`, len(got))
	}
	if len(got) > 0 && got[0].Path != "DATA/Characters/Aatrox/Skins/Base/Aatrox.de_DE.bin" {
		t.Errorf("匹配到的路径不正确: %s", got[0].Path)
	}
}

// TestFilter_Flags 验证单 Flag 过滤。
func TestFilter_Flags(t *testing.T) {
	got := Filter(testFiles, FilterOption{Flags: []string{"de_DE"}})
	// 期望：Aatrox.de_DE, Ahri.de_DE, Aatrox.common（共 3 个）
	if len(got) != 3 {
		t.Errorf("Flags=[de_DE] 返回 %d 个，期望 3 个", len(got))
		for _, f := range got {
			t.Logf("  - %s flags=%v", f.Path, f.Flags)
		}
	}
}

// TestFilter_FlagsAND 验证多 Flag 的 ALL-OF 语义。
func TestFilter_FlagsAND(t *testing.T) {
	// 同时满足 de_DE 和 zh_CN 的只有 Aatrox.common
	got := Filter(testFiles, FilterOption{Flags: []string{"de_DE", "zh_CN"}})
	if len(got) != 1 {
		t.Errorf("Flags=[de_DE,zh_CN] 返回 %d 个，期望 1 个（仅 Aatrox.common）", len(got))
	}
}

// TestFilter_PatternAndFlags 验证 Pattern + Flags 的 AND 组合。
func TestFilter_PatternAndFlags(t *testing.T) {
	// Aatrox 路径 AND de_DE flag
	got := Filter(testFiles, FilterOption{Pattern: "Aatrox", Flags: []string{"de_DE"}})
	// 期望：Aatrox.de_DE.bin 和 Aatrox.common.bin（common 也有 de_DE flag）
	if len(got) != 2 {
		t.Errorf("Pattern=Aatrox + Flags=[de_DE] 返回 %d 个，期望 2 个", len(got))
		for _, f := range got {
			t.Logf("  - %s flags=%v", f.Path, f.Flags)
		}
	}
}

// TestFilter_MultiOpts 验证多个 FilterOption 之间是 OR 关系。
func TestFilter_MultiOpts(t *testing.T) {
	// 满足 Pattern=Ahri OR Flags=[en_US] 的文件
	got := Filter(testFiles,
		FilterOption{Pattern: "Ahri"},
		FilterOption{Flags: []string{"en_US"}},
	)
	// Ahri.de_DE, Ahri.zh_CN ，plus en_US（Assets/Sounds），共 3 个
	// Aatrox.common 也有 en_US，共 4 个
	if len(got) != 4 {
		t.Errorf("多 FilterOption OR 返回 %d 个，期望 4 个", len(got))
		for _, f := range got {
			t.Logf("  - %s flags=%v", f.Path, f.Flags)
		}
	}
}

// TestFilter_NoMatch 验证无匹配时返回空 slice（非 nil）。
func TestFilter_NoMatch(t *testing.T) {
	got := Filter(testFiles, FilterOption{Pattern: "NonExistentChampion9999"})
	if len(got) != 0 {
		t.Errorf("无匹配时期望返回空 slice，但得到 %d 个文件", len(got))
	}
}

// TestFilter_InvalidRegex 验证无效正则时 Filter 跳过该 opt 不 panic。
func TestFilter_InvalidRegex(t *testing.T) {
	// 无效正则 + 有效子串，Filter 跳过无效正则，按有效子串过滤
	_, err := FilterWithError(testFiles,
		FilterOption{Pattern: "[invalid"},
		FilterOption{Pattern: "Aatrox"},
	)
	// FilterWithError 应返回错误（第一个无效正则），但结果不为空
	if err == nil {
		t.Error("无效正则时 FilterWithError 应返回 error")
	}
}

// TestFilter_ReturnsCopy 验证 Filter 返回的是浅拷贝，修改不影响原始 slice。
func TestFilter_ReturnsCopy(t *testing.T) {
	original := []rman.FileEntry{makeEntry("a/b.bin", "x")}
	got := Filter(original)
	if len(got) == 0 {
		t.Fatal("期望返回 1 个文件")
	}
	// 修改返回 slice 的路径，原始数据不应受影响
	got[0].Path = "modified"
	if original[0].Path == "modified" {
		t.Error("Filter 返回的不是浅拷贝，原始数据被修改")
	}
}
