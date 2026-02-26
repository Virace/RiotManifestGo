package core

import (
	"regexp"
	"strings"

	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// FilterOption 定义一组过滤条件，条件之间是 AND 关系。
//
// 使用示例：
//
//	// 仅按路径子串筛选（大小写不敏感）
//	FilterOption{Pattern: "Aatrox"}
//
//	// 仅按 Flag 筛选
//	FilterOption{Flags: []string{"de_DE"}}
//
//	// 路径关键词 + Flag 组合（AND）
//	FilterOption{Pattern: "Aatrox", Flags: []string{"de_DE"}}
//
//	// 精确文件名（含区域）
//	FilterOption{Pattern: "Aatrox.de_DE"}
type FilterOption struct {
	// Pattern 用于匹配 FileEntry.Path（完整相对路径）。
	//
	// 匹配规则：
	//   - 若 Pattern 包含正则元字符（.*+?()[]{}^$|\），则视为 regexp.Regexp 匹配
	//   - 否则作为子串匹配（大小写不敏感，路径中包含该子串即匹配）
	//   - 空字符串表示不过滤路径
	Pattern string

	// Flags 白名单：支持逗号分隔的 AND 逻辑与管道符分隔的 OR 逻辑。
	//
	// 例如 Flags = []string{"de_DE", "ja_JP|ko_KR"}
	// 表示文件必须满足属于 de_DE 区域（AND），且必须属于 ja_JP 或 ko_KR 区域（OR）。
	// 空切片表示不过滤 Flags。
	Flags []string
}

// regexMetaChars 是用于判断 Pattern 是否为正则的元字符集合。
const regexMetaChars = `.*+?()[]{}^$|\`

// isRegexPattern 判断 pattern 字符串是否包含正则元字符，需要按 regexp 处理。
func isRegexPattern(pattern string) bool {
	return strings.ContainsAny(pattern, regexMetaChars)
}

// compiledFilter 是 FilterOption 的预编译内部表示，避免在循环中重复编译。
type compiledFilter struct {
	// re 当 Pattern 是正则时非 nil
	re *regexp.Regexp
	// substring 当 Pattern 是子串时非空（已转为小写）
	substring string
	// flags 所有必须包含的 flag 条件组（已转为小写）。文件需满足所有组（AND），每组内满足任一（OR）。
	flags [][]string
	// hasPattern 是否有路径过滤条件
	hasPattern bool
}

// compile 将 FilterOption 预编译为 compiledFilter。
// Pattern 编译失败（无效正则）时返回 error。
func (opt FilterOption) compile() (compiledFilter, error) {
	cf := compiledFilter{
		hasPattern: opt.Pattern != "",
	}

	if opt.Pattern != "" {
		if isRegexPattern(opt.Pattern) {
			re, err := regexp.Compile(opt.Pattern)
			if err != nil {
				return cf, err
			}
			cf.re = re
		} else {
			cf.substring = strings.ToLower(opt.Pattern)
		}
	}

	cf.flags = make([][]string, len(opt.Flags))
	for i, f := range opt.Flags {
		parts := strings.Split(f, "|")
		group := make([]string, len(parts))
		for j, p := range parts {
			group[j] = strings.ToLower(p)
		}
		cf.flags[i] = group
	}

	return cf, nil
}

// matchPath 检测 entry.Path 是否满足路径过滤条件。
func (cf *compiledFilter) matchPath(path string) bool {
	if !cf.hasPattern {
		return true
	}
	if cf.re != nil {
		return cf.re.MatchString(path)
	}
	return strings.Contains(strings.ToLower(path), cf.substring)
}

// matchFlags 检测 entry.Flags 是否满足所有 flag 条件组的要求。
func (cf *compiledFilter) matchFlags(entryFlags []string) bool {
	if len(cf.flags) == 0 {
		return true
	}

	// 将 entry 的 flags 全部转为小写，放入集合，以便 O(1) 查找
	flagSet := make(map[string]struct{}, len(entryFlags))
	for _, f := range entryFlags {
		flagSet[strings.ToLower(f)] = struct{}{}
	}

	for _, reqGroup := range cf.flags {
		groupMatched := false
		for _, required := range reqGroup {
			if _, ok := flagSet[required]; ok {
				groupMatched = true
				break
			}
		}
		if !groupMatched {
			return false
		}
	}
	return true
}

// matches 检测 entry 是否满足该过滤条件（Path AND Flags）。
func (cf *compiledFilter) matches(entry rman.FileEntry) bool {
	return cf.matchPath(entry.Path) && cf.matchFlags(entry.Flags)
}

// Filter 从 files 中筛选满足条件的 FileEntry。
//
// 语义规则：
//   - 多个 FilterOption 之间是 OR 关系（满足任意一个即入选）
//   - 单个 FilterOption 内 Pattern 与 Flags 是 AND 关系
//   - 不传任何 opts 时返回全部文件（相当于无过滤）
//
// 若某个 FilterOption.Pattern 是无效正则，该 Option 会被跳过（不报错），
// 以保证健壮性。生产场景建议改用 FilterWithError 获取编译错误。
func Filter(files []rman.FileEntry, opts ...FilterOption) []rman.FileEntry {
	result, _ := FilterWithError(files, opts...)
	return result
}

// FilterWithError 与 Filter 相同，但返回第一个正则编译错误。
// 编译失败的 FilterOption 会被跳过继续处理其余 opts。
func FilterWithError(files []rman.FileEntry, opts ...FilterOption) ([]rman.FileEntry, error) {
	// 无过滤条件：返回全部文件
	if len(opts) == 0 {
		// 返回浅拷贝，避免调用方修改原始 slice
		result := make([]rman.FileEntry, len(files))
		copy(result, files)
		return result, nil
	}

	// 预编译所有 FilterOption
	compiled := make([]compiledFilter, 0, len(opts))
	var firstErr error
	for _, opt := range opts {
		cf, err := opt.compile()
		if err != nil && firstErr == nil {
			firstErr = err
		}
		// 编译失败时 cf 是零值（hasPattern=false, flags=nil），会匹配所有，跳过该 opt
		if err == nil {
			compiled = append(compiled, cf)
		}
	}

	// 如果所有 opts 都编译失败，返回空结果和错误
	if len(compiled) == 0 {
		return nil, firstErr
	}

	result := make([]rman.FileEntry, 0, len(files)/2)
	for _, entry := range files {
		// OR 语义：满足任意一个 compiledFilter 即入选
		for i := range compiled {
			if compiled[i].matches(entry) {
				result = append(result, entry)
				break
			}
		}
	}
	return result, firstErr
}
