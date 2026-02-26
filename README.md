# RiotManifestGo

Riot 游戏资源清单（RMAN / `.manifest`）解析与下载工具的 Go 实现。

> 致谢 [Morilli/ManifestDownloader](https://github.com/Morilli/ManifestDownloader)，本项目参考其 C 实现进行了 Go 语言重写。

## 功能

- **RMAN 清单解析**：完整解析 `.manifest` 文件（FlatBuffers + ZSTD 压缩）
- **灵活筛选**：支持路径匹配（子串/正则）和 Flags 过滤（语言/平台）
- **高速并发下载**：多 Worker 并行，HTTP Range 合并，ZSTD 流式解压
- **哈希校验**：支持 SHA256、SHA512、HKDF、Blake3 四种校验算法
- **纯 Go 实现**：无 CGO 依赖，交叉编译友好

## 快速开始

```bash
go build -o manifest-cli ./cmd/manifest-cli/
```

### 列出文件

```bash
# 本地 manifest
./manifest-cli game.manifest -list

# 远程 manifest
./manifest-cli https://lol.secure.dyn.riotcdn.net/channels/public/releases/XXXXX.manifest -list

# 筛选 Aatrox 的 de_DE 和 zh_CN 文件（OR 运算）
./manifest-cli game.manifest -list -p Aatrox -f "de_DE|zh_CN"

# 混合筛选：必须是 Windows 平台，且语言是 ja_JP 或 ko_KR
./manifest-cli game.manifest -list -f "windows,ja_JP|ko_KR"
```

### 下载文件

```bash
# 下载匹配的 DLL 文件（使用默认 CDN）
./manifest-cli game.manifest -p "\.dll" -o ./output

# 指定 CDN + 保存日志
./manifest-cli game.manifest -p "\.dll" -o ./output -u https://cdn.example.com/bundles -log download.log
```

## 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `<manifest>` | manifest 文件路径或 URL（**必需**，位置参数） | - |
| `-list` | 仅列出文件，不下载 | `false` |
| `-p` | 路径筛选（子串或正则） | - |
| `-f` | Flag 过滤，逗号代表与，管道符`\|`代表或（如 `de_DE,windows` 或 `ja_JP\|ko_KR`） | - |
| `-o` | 输出目录 | `./output` |
| `-u` | CDN Bundle 基础 URL | LoL CDN |
| `-w` | 并发 Worker 数 | `16` |
| `-n` | 列表最大显示数量（-1=全部） | `20` |
| `-s` | 静默模式（仅输出错误） | `false` |
| `-v` | 输出等级（0=进度条, 1-3=递增详细度） | `0` |
| `-log` | 保存下载日志 | - |
| `-retry` | Bundle 下载失败最大重试次数 | `3` |

## 架构

```
cmd/manifest-cli/     CLI 入口
pkg/rman/             RMAN 解析器（FlatBuffers + ZSTD）
pkg/core/             调度核心（Filter → Map → Schedule → Download）
internal/zstream/     ZSTD 解压 + 哈希校验
internal/netpool/     HTTP Range 客户端
internal/fswriter/    LRU 文件句柄池
```

## 依赖

| 包 | 用途 |
|----|------|
| `github.com/klauspost/compress/zstd` | ZSTD 解压（纯 Go） |
| `lukechampine.com/blake3` | Blake3 哈希（纯 Go） |

## 许可证

MIT
