// Package rman 实现了拳头游戏 RMAN（.manifest）文件的解析器。
package rman

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// ParseFile 从文件路径解析 RMAN manifest 文件，返回 *Manifest 或 error。
func ParseFile(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开 manifest 文件失败: %w", err)
	}
	defer f.Close()
	return Parse(f)
}

// Parse 从 io.Reader 解析 RMAN manifest 文件。
func Parse(r io.Reader) (*Manifest, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("读取 manifest 数据失败: %w", err)
	}
	return parseBytes(data)
}

// parseBytes 从字节切片解析 manifest。
func parseBytes(data []byte) (*Manifest, error) {
	// 1. 解析文件头
	hdr, err := parseHeader(data)
	if err != nil {
		return nil, err
	}

	// 2. ZSTD 解压 Body
	compressedEnd := uint32(hdr.ContentOffset) + hdr.CompressedSize
	if compressedEnd > uint32(len(data)) {
		return nil, fmt.Errorf("manifest 数据截断：期望 %d 字节，实际 %d 字节",
			compressedEnd, len(data))
	}
	compressedBody := data[hdr.ContentOffset:compressedEnd]

	decoder, err := zstd.NewReader(bytes.NewReader(compressedBody))
	if err != nil {
		return nil, fmt.Errorf("创建 ZSTD 解码器失败: %w", err)
	}
	defer decoder.Close()

	body := make([]byte, 0, hdr.UncompressedSize)
	buf := make([]byte, 1<<16)
	for {
		n, readErr := decoder.Read(buf)
		body = append(body, buf[:n]...)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("ZSTD 解压失败: %w", readErr)
		}
	}

	if uint32(len(body)) != hdr.UncompressedSize {
		return nil, fmt.Errorf("解压大小不匹配：期望 %d，实际 %d",
			hdr.UncompressedSize, len(body))
	}

	// 3. 解析 FlatBuffers Body
	files, err := parseBody(body)
	if err != nil {
		return nil, fmt.Errorf("解析 FlatBuffers Body 失败: %w", err)
	}

	return &Manifest{
		ManifestID: hdr.ManifestID,
		Files:      files,
	}, nil
}

// parseHeader 解析 RMAN 文件头（28 字节）。
//
//	偏移  大小   字段
//	0     4      魔数 "RMAN"
//	4     1      MajorVersion（必须为 2）
//	5     1      MinorVersion（0 或 1 已测试）
//	6     2      保留
//	8     4      contentOffset
//	12    4      compressedSize
//	16    8      manifestID（uint64）
//	24    4      uncompressedSize
func parseHeader(data []byte) (*Header, error) {
	const headerSize = 28
	if len(data) < headerSize {
		return nil, fmt.Errorf("manifest 文件过短：%d 字节（至少需要 %d 字节）", len(data), headerSize)
	}

	// 魔数校验
	if !bytes.Equal(data[0:4], rmanMagic[:]) {
		return nil, fmt.Errorf("无效魔数：%q（期望 \"RMAN\"）", data[0:4])
	}

	major := data[4]
	minor := data[5]

	// 主版本号必须为 2
	if major != 2 {
		return nil, fmt.Errorf("不支持的 RMAN 主版本号：%d（仅支持 2）", major)
	}
	// 次版本号 0/1 已验证，其他版本警告但继续
	if minor != 0 && minor != 1 {
		log.Printf("[rman] 警告：未知次版本号 %d，尝试继续解析", minor)
	}

	le := binary.LittleEndian
	return &Header{
		MajorVersion:     major,
		MinorVersion:     minor,
		ContentOffset:    le.Uint32(data[8:12]),
		CompressedSize:   le.Uint32(data[12:16]),
		ManifestID:       le.Uint64(data[16:24]),
		UncompressedSize: le.Uint32(data[24:28]),
	}, nil
}

// parseBody 解析 ZSTD 解压后的 FlatBuffers Body。
func parseBody(body []byte) ([]FileEntry, error) {
	// FlatBuffers 根对象偏移
	rootOffset := fbRootOffset(body)

	// 解析 Parameters 列表（field index 5）
	// 需要先解析 parameters，因为 FileEntry 通过 param_index 引用哈希类型
	params, err := parseParameters(body, rootOffset)
	if err != nil {
		return nil, fmt.Errorf("解析 Parameters 失败: %w", err)
	}

	// 解析 Language 列表（field index 1）
	// 建立 langMap: index(0-based) -> name，用于 language_mask 解码
	langMap, err := parseLanguages(body, rootOffset)
	if err != nil {
		return nil, fmt.Errorf("解析 Languages 失败: %w", err)
	}

	// 解析 Directory 列表（field index 3）
	// 建立 dirMap: directory_id -> dirEntry，用于路径重建
	dirMap, err := parseDirectories(body, rootOffset)
	if err != nil {
		return nil, fmt.Errorf("解析 Directories 失败: %w", err)
	}

	// 解析 Bundle 列表（field index 0）
	// 建立全局 Chunk 索引 chunkMap: chunk_id -> *ChunkInfo
	// 关键：bundle_offset 在此阶段通过累加 compressed_size 推算
	chunkMap, err := parseBundles(body, rootOffset)
	if err != nil {
		return nil, fmt.Errorf("解析 Bundles 失败: %w", err)
	}

	// 解析 FileEntry 列表（field index 2）
	files, err := parseFileEntries(body, rootOffset, chunkMap, dirMap, langMap, params)
	if err != nil {
		return nil, fmt.Errorf("解析 FileEntries 失败: %w", err)
	}

	return files, nil
}

// parseParameters 解析根对象的 parameters 字段（field index 5）。
// Parameters 对象包含哈希类型、分块大小参数。
func parseParameters(body []byte, rootOffset uint32) ([]parameters, error) {
	dataStart, count := fbVectorInfo(body, rootOffset, 5)
	if count == 0 {
		return nil, nil
	}

	params := make([]parameters, count)
	for i := uint32(0); i < count; i++ {
		tableOffset := fbVectorTableAt(body, dataStart, i)
		params[i] = parameters{
			HashType:        HashType(fbReadUint8(body, tableOffset, 1, 0)),
			MinChunkSize:    fbReadUint32(body, tableOffset, 2, 0),
			MaxChunkSize:    fbReadUint32(body, tableOffset, 3, 0),
			MaxUncompressed: fbReadUint32(body, tableOffset, 4, 0),
		}
	}
	return params, nil
}

// parseLanguages 解析 Language 列表（field index 1），返回 0-based index -> name 的映射。
// language_mask 的第 i 位（0-based）对应 language_id = i+1 的语言。
func parseLanguages(body []byte, rootOffset uint32) (map[int]string, error) {
	dataStart, count := fbVectorInfo(body, rootOffset, 1)
	langMap := make(map[int]string)
	if count == 0 {
		return langMap, nil
	}

	for i := uint32(0); i < count; i++ {
		tableOffset := fbVectorTableAt(body, dataStart, i)
		// Language（Flag）对象：field 0 = flag_id(uint8), field 1 = name(string)
		// 参考 CDTB patcher.py _parse_flag: unpack('<xxxBl') 明确 flag_id 为 uint8
		// language_mask 第 i 位（0-based）= flag_id=i+1，所以 bit index = flag_id - 1
		langID := fbReadUint8(body, tableOffset, 0, 0)
		name := fbReadString(body, tableOffset, 1)
		if langID > 0 {
			langMap[int(langID-1)] = name // bit index = langID - 1
		}
	}
	return langMap, nil
}

// parseDirectories 解析 Directory 列表（field index 3）,
// 返回 directory_id -> dirEntry 的映射，用于路径重建。
func parseDirectories(body []byte, rootOffset uint32) (map[uint64]dirEntry, error) {
	dataStart, count := fbVectorInfo(body, rootOffset, 3)
	dirMap := make(map[uint64]dirEntry, count)
	if count == 0 {
		return dirMap, nil
	}

	for i := uint32(0); i < count; i++ {
		tableOffset := fbVectorTableAt(body, dataStart, i)
		// Directory 对象：field 0=directory_id, field 1=parent_id, field 2=name
		dirID := fbReadUint64(body, tableOffset, 0, 0)
		parentID := fbReadUint64(body, tableOffset, 1, 0)
		name := fbReadString(body, tableOffset, 2)
		dirMap[dirID] = dirEntry{Name: name, ParentID: parentID}
	}
	return dirMap, nil
}

// parseBundles 解析 Bundle 列表（field index 0），建立全局 Chunk 索引。
//
// 核心逻辑：
// bundle_offset 不存储在 FlatBuffers 中，由 Parser 对同一 Bundle 内 Chunk 顺序
// 累加 compressed_size 推算：
//
//	offset := uint32(0)
//	for each chunk in bundle:
//	    chunk.BundleOffset = offset
//	    offset += chunk.CompressedSize
func parseBundles(body []byte, rootOffset uint32) (map[uint64]*ChunkInfo, error) {
	dataStart, count := fbVectorInfo(body, rootOffset, 0)
	// 预分配：粗略估计平均每 bundle 有 ~100 chunks
	chunkMap := make(map[uint64]*ChunkInfo, int(count)*100)
	if count == 0 {
		return chunkMap, nil
	}

	for i := uint32(0); i < count; i++ {
		bundleTableOffset := fbVectorTableAt(body, dataStart, i)
		// Bundle 对象：field 0=bundle_id, field 1=chunks(vector)
		bundleID := fbReadUint64(body, bundleTableOffset, 0, 0)

		chunkDataStart, chunkCount := fbVectorInfo(body, bundleTableOffset, 1)
		if chunkCount == 0 {
			continue
		}

		// 累加 bundle_offset（这是 Parser 层自行推算的关键逻辑）
		var bundleOffset uint32 = 0
		for j := uint32(0); j < chunkCount; j++ {
			chunkTableOffset := fbVectorTableAt(body, chunkDataStart, j)
			// Chunk 对象：field 0=chunk_id, field 1=compressed_size, field 2=uncompressed_size
			chunkID := fbReadUint64(body, chunkTableOffset, 0, 0)
			compSize := fbReadUint32(body, chunkTableOffset, 1, 0)
			uncompSize := fbReadUint32(body, chunkTableOffset, 2, 0)

			chunk := &ChunkInfo{
				ChunkID:          chunkID,
				BundleID:         bundleID,
				BundleOffset:     bundleOffset, // 由 Parser 累加推算，非 FlatBuffers 原生字段
				CompressedSize:   compSize,
				UncompressedSize: uncompSize,
				// HashType 在 parseFileEntries 阶段根据 param_index 填充
			}
			chunkMap[chunkID] = chunk

			// 累加：下一个 chunk 的 offset = 当前 offset + 当前 compressed_size
			bundleOffset += compSize
		}
	}
	return chunkMap, nil
}

// parseFileEntries 解析 FileEntry 列表（field index 2），
// 关联 Chunk 索引、重建路径、解码语言掩码。
func parseFileEntries(
	body []byte,
	rootOffset uint32,
	chunkMap map[uint64]*ChunkInfo,
	dirMap map[uint64]dirEntry,
	langMap map[int]string,
	params []parameters,
) ([]FileEntry, error) {
	dataStart, count := fbVectorInfo(body, rootOffset, 2)
	if count == 0 {
		return nil, nil
	}

	files := make([]FileEntry, 0, count)

	for i := uint32(0); i < count; i++ {
		tableOffset := fbVectorTableAt(body, dataStart, i)

		// FileEntry 字段（按 field index）：
		// 0=file_entry_id, 1=directory_id, 2=file_size, 3=name, 4=language_mask
		// 7=chunk_ids(vector), 9=link, 11=param_index
		dirID := fbReadUint64(body, tableOffset, 1, 0)
		fileSize := fbReadUint64(body, tableOffset, 2, 0)
		name := fbReadString(body, tableOffset, 3)
		languageMask := fbReadUint64(body, tableOffset, 4, 0)
		link := fbReadString(body, tableOffset, 9)
		paramIndex := fbReadUint8(body, tableOffset, 11, 0)

		// 确定该文件使用的 HashType
		var hashType HashType
		if int(paramIndex) < len(params) {
			hashType = params[paramIndex].HashType
		}

		// 解码 flag_mask：第 i 位（0-based）= 1 时，表示属于 flagMap[i]
		var flags []string
		for bit := 0; bit < 64; bit++ {
			if languageMask&(1<<uint(bit)) != 0 {
				if lang, ok := langMap[bit]; ok {
					flags = append(flags, lang)
				}
			}
		}

		// 重建完整路径
		fullPath := buildPath(dirMap, dirID, name)

		// 关联 chunk_ids（field index 7）：vector of uint64
		chunkIDsStart, chunkCount := fbVectorInfo(body, tableOffset, 7)
		chunks := make([]ChunkInfo, 0, chunkCount)
		for j := uint32(0); j < chunkCount; j++ {
			chunkID := fbVectorUint64At(body, chunkIDsStart, j)
			if ci, ok := chunkMap[chunkID]; ok {
				// 复制 ChunkInfo 并填充 HashType（每个文件可能有不同的 param_index）
				c := *ci
				c.HashType = hashType
				chunks = append(chunks, c)
			}
			// 如果 chunk 不在 chunkMap 中，跳过（数据损坏时的防御）
		}

		files = append(files, FileEntry{
			Path:     fullPath,
			Link:     link,
			FileSize: fileSize,
			Flags:    flags,
			Chunks:   chunks,
		})
	}

	return files, nil
}

// buildPath 向上递归遍历目录树，重建文件的完整相对路径。
//
//	最终路径 = "grandparent_dir/parent_dir/filename"
//
// 传入 dirID=0 表示根目录，直接返回 name。
func buildPath(dirMap map[uint64]dirEntry, dirID uint64, name string) string {
	if dirID == 0 {
		return name
	}

	// 向上收集路径段（最多 64 层防止无限循环）
	parts := make([]string, 0, 8)
	visited := make(map[uint64]bool)
	current := dirID

	for current != 0 {
		if visited[current] {
			// 检测到目录树循环引用，防止死循环
			break
		}
		visited[current] = true

		entry, ok := dirMap[current]
		if !ok {
			break
		}
		parts = append(parts, entry.Name)
		current = entry.ParentID
	}

	// 反转路径段（从根到叶）
	for left, right := 0, len(parts)-1; left < right; left, right = left+1, right-1 {
		parts[left], parts[right] = parts[right], parts[left]
	}

	parts = append(parts, name)
	return strings.Join(parts, "/")
}
