// Package rman 中的 flatbuffers.go 提供手动解析 FlatBuffers 二进制格式的工具函数。
//
// 不使用 FlatBuffers 代码生成器，直接按照 FlatBuffers 规范手动读取 vtable 偏移。
//
// FlatBuffers 关键布局（Little Endian）：
//
//	root offset @ buf[0:4]：指向根 Table 对象的偏移量
//	Table @ tableOffset：
//	  [tableOffset]   = int32 vtable_soffset（负数，tableOffset - vtable_soffset = vtableOffset）
//	  vtable @ vtableOffset：
//	    [+0] = uint16 vtable length（含自身）
//	    [+2] = uint16 object data size
//	    [+4 + fieldIndex*2] = uint16 fieldOffset（相对 tableOffset，0 表示字段不存在）
package rman

import (
	"encoding/binary"
	"fmt"
)

// fbLE 是 FlatBuffers 使用的字节序（小端）
var fbLE = binary.LittleEndian

// fbRootOffset 读取 FlatBuffers 根对象的偏移量（buf[0:4] 存储的 uint32 offset）。
func fbRootOffset(buf []byte) uint32 {
	return fbLE.Uint32(buf[0:4])
}

// fbGetFieldOffset 从 vtable 读取指定字段相对于 tableOffset 的偏移量。
// 如果字段不存在（vtable 中存储 0），返回 0。
//
// 从 vtable 读取指定字段的偏移量。如果字段不存在（vtable 中存储 0），返回 0。
func fbGetFieldOffset(buf []byte, tableOffset uint32, fieldIndex int) uint32 {
	// vtable soffset 是 int32（有符号），存在 tableOffset 位置
	soffset := int32(fbLE.Uint32(buf[tableOffset : tableOffset+4]))
	vtableOffset := uint32(int32(tableOffset) - soffset)

	// vtable 长度（uint16），确保字段在 vtable 范围内
	vtableLen := uint32(fbLE.Uint16(buf[vtableOffset : vtableOffset+2]))
	fieldByteIndex := uint32(4 + fieldIndex*2)
	if fieldByteIndex+2 > vtableLen {
		return 0 // 字段不存在
	}

	fieldOff := uint32(fbLE.Uint16(buf[vtableOffset+fieldByteIndex : vtableOffset+fieldByteIndex+2]))
	return fieldOff // 0 表示字段不存在，非 0 表示相对 tableOffset 的偏移
}

// fbGetFieldAbsOffset 返回字段的绝对偏移，若字段不存在返回 0。
func fbGetFieldAbsOffset(buf []byte, tableOffset uint32, fieldIndex int) uint32 {
	rel := fbGetFieldOffset(buf, tableOffset, fieldIndex)
	if rel == 0 {
		return 0
	}
	return tableOffset + rel
}

// fbReadUint8 读取字段 fieldIndex 的 uint8 值，字段不存在返回 defaultVal。
func fbReadUint8(buf []byte, tableOffset uint32, fieldIndex int, defaultVal uint8) uint8 {
	abs := fbGetFieldAbsOffset(buf, tableOffset, fieldIndex)
	if abs == 0 {
		return defaultVal
	}
	return buf[abs]
}

// fbReadUint32 读取字段 fieldIndex 的 uint32 值，字段不存在返回 defaultVal。
func fbReadUint32(buf []byte, tableOffset uint32, fieldIndex int, defaultVal uint32) uint32 {
	abs := fbGetFieldAbsOffset(buf, tableOffset, fieldIndex)
	if abs == 0 {
		return defaultVal
	}
	return fbLE.Uint32(buf[abs : abs+4])
}

// fbReadUint64 读取字段 fieldIndex 的 uint64 值，字段不存在返回 defaultVal。
func fbReadUint64(buf []byte, tableOffset uint32, fieldIndex int, defaultVal uint64) uint64 {
	abs := fbGetFieldAbsOffset(buf, tableOffset, fieldIndex)
	if abs == 0 {
		return defaultVal
	}
	return fbLE.Uint64(buf[abs : abs+8])
}

// fbReadString 读取字段 fieldIndex 的字符串值，字段不存在返回 ""。
// FlatBuffers 字符串：uint32 length + UTF-8 bytes + '\0'
func fbReadString(buf []byte, tableOffset uint32, fieldIndex int) string {
	abs := fbGetFieldAbsOffset(buf, tableOffset, fieldIndex)
	if abs == 0 {
		return ""
	}
	// abs 处存储的是 string 对象的相对偏移（referenceOffset 格式）
	strOffset := abs + fbLE.Uint32(buf[abs:abs+4])
	strLen := fbLE.Uint32(buf[strOffset : strOffset+4])
	return string(buf[strOffset+4 : strOffset+4+strLen])
}

// fbVectorInfo 返回 vector 字段的（向量数据起始绝对偏移, 元素数量）。
// 若字段不存在，返回 (0, 0)。
// FlatBuffers vector：uint32 count + 元素数组（table 型元素用 referenceOffset 存储）
func fbVectorInfo(buf []byte, tableOffset uint32, fieldIndex int) (dataStart uint32, count uint32) {
	abs := fbGetFieldAbsOffset(buf, tableOffset, fieldIndex)
	if abs == 0 {
		return 0, 0
	}
	// abs 处存储 vector 对象的 uoffset
	vecOffset := abs + fbLE.Uint32(buf[abs:abs+4])
	count = fbLE.Uint32(buf[vecOffset : vecOffset+4])
	dataStart = vecOffset + 4
	return dataStart, count
}

// fbVectorUint64At 读取 uint64 类型 vector 的第 i 个元素。
// 对于标量 vector，元素直接内联存储（每元素 8 字节）。
func fbVectorUint64At(buf []byte, dataStart uint32, i uint32) uint64 {
	pos := dataStart + i*8
	return fbLE.Uint64(buf[pos : pos+8])
}

// fbVectorTableAt 返回 Table 类型 vector 的第 i 个元素的绝对 tableOffset。
// 对于 Table vector，每个元素是 4 字节 referenceOffset。
func fbVectorTableAt(buf []byte, dataStart uint32, i uint32) uint32 {
	pos := dataStart + i*4
	ref := fbLE.Uint32(buf[pos : pos+4])
	return pos + ref
}

// fbBoundsCheck 校验访问范围，防止越界 panic，供调试使用。
func fbBoundsCheck(buf []byte, offset, size uint32, label string) error {
	if uint32(len(buf)) < offset+size {
		return fmt.Errorf("flatbuffers 越界：%s @ offset=%d size=%d bufLen=%d",
			label, offset, size, len(buf))
	}
	return nil
}
