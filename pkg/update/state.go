// Package update 实现本地增量更新的"版本锚点"：将成功获取到的原始 manifest
// 字节存档到 <output>/.rman/manifests/ 下，并通过 <output>/.rman/installed.json
// 记录当前 manifest 指针与已确认的受管理文件覆盖，供安装流程发现旧版本并限定
// 文件级跳过、移动复用和清理权限。
//
// installed.json 的字段/格式是与姊妹 Python 项目共享的固定契约（schema=2），
// 修改需双侧同步，不得随意调整字段名或格式。
package update

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// schemaVersion 是当前实现能够识别并写出的 installed.json schema 版本。
const schemaVersion = 2

// legacySchemaVersion 是旧版 manifest 级状态。它没有文件覆盖信息，因此只能
// 作为旧清单定位提示，不能授权跳过、移动复用或清理。
const legacySchemaVersion = 1

// InstalledState 对应 installed.json 的内容，字段与 JSON 标签均为跨语言共享契约，
// 不可随意改名或调整顺序（顺序决定了 MarshalIndent 输出的字段顺序）。
type InstalledState struct {
	Schema       int      `json:"schema"`
	ManifestID   string   `json:"manifest_id"`   // %016X
	ManifestFile string   `json:"manifest_file"` // 相对 .rman/，JSON 中始终使用正斜杠
	Source       string   `json:"source"`
	UpdatedAt    string   `json:"updated_at"` // RFC3339 UTC
	Files        []string `json:"files"`      // 当前清单下已确认的受管理文件，正斜杠、排序去重
}

// Archive 管理 <output>/.rman 下的 manifest 存档与 installed.json 状态文件。
type Archive struct {
	root string // <output>/.rman
}

// NewArchive 创建一个以 outputDir/.rman 为根目录的 Archive。
func NewArchive(outputDir string) *Archive {
	return &Archive{root: filepath.Join(outputDir, ".rman")}
}

// installedPath 返回 installed.json 的绝对路径。
func (a *Archive) installedPath() string {
	return filepath.Join(a.root, "installed.json")
}

// manifestsDir 返回存档 manifest 文件所在目录的绝对路径。
func (a *Archive) manifestsDir() string {
	return filepath.Join(a.root, "manifests")
}

// LoadInstalled 读取并解析 installed.json。
//
// installed.json 缺失、内容不是合法 JSON、或 schema 字段不是 1/2，均视为
// "无有效状态"，返回 (nil, nil)（而非错误）。schema 1 会被读回，但 Files
// 始终为空，确保它不能授权任何文件级受管理动作。只有真正的 I/O 错误（如权限
// 问题）才会返回非 nil 的 error。
func (a *Archive) LoadInstalled() (*InstalledState, error) {
	data, err := os.ReadFile(a.installedPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("读取 installed.json 失败: %w", err)
	}

	var state InstalledState
	if err := json.Unmarshal(data, &state); err != nil {
		// 损坏的 JSON 视为无状态，而非报错：上层应退化为"无旧版本锚点"。
		return nil, nil
	}
	switch state.Schema {
	case legacySchemaVersion:
		state.Files = nil
	case schemaVersion:
		if state.Files == nil {
			// schema 2 必须显式携带 files 数组；字段缺失或 JSON null 都不能
			// 与可信的空覆盖混为一谈。
			return nil, nil
		}
		files, err := normalizeManagedFiles(state.Files)
		if err != nil {
			// 路径不符合 schema 2 契约时整份状态失去授权能力，按无状态处理。
			return nil, nil
		}
		state.Files = files
	default:
		// 未识别的 schema（包括未来更高版本）同样视为无状态，向前兼容。
		return nil, nil
	}
	manifestFiles, err := normalizeManagedFiles([]string{state.ManifestFile})
	if err != nil {
		return nil, nil
	}
	state.ManifestFile = manifestFiles[0]
	return &state, nil
}

// HasInstalledState 报告 installed.json 是否实际存在。它不解析内容，因此损坏、
// legacy 或未来 schema 也会被视为"这是一个受管理根"，供 CLI 阻止默认下载模式
// 静默写入并破坏已有安装状态。
func (a *Archive) HasInstalledState() (bool, error) {
	_, err := os.Stat(a.installedPath())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("检查 installed.json 失败: %w", err)
}

// InstalledManifestPath 返回当前 installed.json 指向的 manifest 存档文件的绝对路径。
// 只有当 installed.json 存在有效状态、且其指向的文件在磁盘上确实存在时，才返回 ok=true。
func (a *Archive) InstalledManifestPath() (string, bool) {
	state, err := a.LoadInstalled()
	if err != nil || state == nil {
		return "", false
	}

	path := filepath.Join(a.root, filepath.FromSlash(state.ManifestFile))
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", false
	}
	return path, true
}

// Save 将原始 manifest 字节存档到 manifests/<%016X>.manifest，并原子更新
// installed.json 使其指向该文件。存档目录只保留本次与上一次（写入前 installed.json
// 指向的）manifest 文件，其余全部删除，避免 manifests/ 目录无限增长。files
// 会先规范化为正斜杠路径并排序去重。
func (a *Archive) Save(manifestID uint64, raw []byte, source string, files []string) error {
	normalizedFiles, err := normalizeManagedFiles(files)
	if err != nil {
		return fmt.Errorf("规范化受管理文件列表失败: %w", err)
	}

	if err := os.MkdirAll(a.manifestsDir(), 0755); err != nil {
		return fmt.Errorf("创建 manifests 目录失败: %w", err)
	}

	// 在覆盖 installed.json 之前先读取旧状态，用于之后清理旧 manifest 文件。
	// 读取失败（含损坏/schema 不识别）时按"无旧状态"处理，不阻塞本次 Save。
	prevState, _ := a.LoadInstalled()

	manifestIDStr := fmt.Sprintf("%016X", manifestID)
	manifestRelPath := "manifests/" + manifestIDStr + ".manifest" // 契约要求 JSON 中始终使用正斜杠
	manifestAbsPath := filepath.Join(a.manifestsDir(), manifestIDStr+".manifest")

	if err := writeFileAtomic(manifestAbsPath, raw, 0644); err != nil {
		return fmt.Errorf("写入 manifest 存档失败: %w", err)
	}

	newState := InstalledState{
		Schema:       schemaVersion,
		ManifestID:   manifestIDStr,
		ManifestFile: manifestRelPath,
		Source:       source,
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339),
		Files:        normalizedFiles,
	}
	payload, err := json.MarshalIndent(newState, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化 installed.json 失败: %w", err)
	}

	if err := writeFileAtomic(a.installedPath(), payload, 0644); err != nil {
		return fmt.Errorf("写入 installed.json 失败: %w", err)
	}

	// installed.json 已成功指向新版本，此时才清理旧 manifest 文件；
	// 清理失败只是残留多余文件，不影响 installed.json 已经正确的指向，因此忽略错误。
	a.pruneManifests(manifestAbsPath, prevState)

	return nil
}

// normalizeManagedFiles 实现 schema 2 的路径契约：manifest 相对路径、正斜杠、
// 唯一且稳定排序。任何不安全路径都会使整次状态读写 fail-safe。
func normalizeManagedFiles(files []string) ([]string, error) {
	unique := make(map[string]struct{}, len(files))
	for _, file := range files {
		normalized := strings.ReplaceAll(file, "\\", "/")
		if normalized == "" || path.IsAbs(normalized) {
			return nil, fmt.Errorf("非法 manifest 相对路径 %q", file)
		}
		for _, part := range strings.Split(normalized, "/") {
			if part == "" || part == "." || part == ".." || strings.Contains(part, ":") {
				return nil, fmt.Errorf("非法 manifest 相对路径 %q", file)
			}
		}
		cleaned := path.Clean(normalized)
		if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			return nil, fmt.Errorf("非法 manifest 相对路径 %q", file)
		}
		unique[cleaned] = struct{}{}
	}

	result := make([]string, 0, len(unique))
	for file := range unique {
		result = append(result, file)
	}
	sort.Strings(result)
	return result, nil
}

// pruneManifests 删除 manifests/ 目录下除 keepPath 与 prev 指向的文件之外的所有文件。
func (a *Archive) pruneManifests(keepPath string, prev *InstalledState) {
	keep := map[string]bool{filepath.Clean(keepPath): true}
	if prev != nil {
		prevPath := filepath.Join(a.root, filepath.FromSlash(prev.ManifestFile))
		keep[filepath.Clean(prevPath)] = true
	}

	entries, err := os.ReadDir(a.manifestsDir())
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		full := filepath.Clean(filepath.Join(a.manifestsDir(), entry.Name()))
		if keep[full] {
			continue
		}
		_ = os.Remove(full)
	}
}

// writeFileAtomic 以"写临时文件 + 原子 rename"的方式写入文件：先将内容写入
// path+".tmp"，成功后再通过 os.Rename 替换到目标路径。写入临时文件失败时不会
// 触碰目标文件；rename 失败时会尝试清理残留的临时文件。
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
