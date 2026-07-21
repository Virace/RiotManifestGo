# RiotManifestGo

Riot 游戏资源清单（RMAN / `.manifest`）解析与下载工具的 Go 实现。

> **鸣谢**
>
> - [Morilli/ManifestDownloader](https://github.com/Morilli/ManifestDownloader) — 清单解析与下载参考其 C 实现进行 Go 语言重写
> - [moonshadow565/rman](https://github.com/moonshadow565/rman) — update 增量更新逻辑参考其 RMAN 实现

## 功能

- **RMAN 清单解析**：完整解析 `.manifest` 文件（FlatBuffers + ZSTD 压缩）
- **灵活筛选**：支持路径匹配（子串/正则）和 Flags 过滤（语言/平台）
- **高速并发下载**：多 Worker 并行，HTTP Range 合并，ZSTD 流式解压
- **哈希校验**：支持 SHA256、SHA512、HKDF、Blake3 四种校验算法
- **增量更新**：自动发现本地存档，按 chunk 级校验只下载新旧清单间真正变化的部分，未变化文件整个跳过、未变化数据本地复用
- **原子写盘**：下载与本地修复统一先写临时文件、完成后原子替换，中断不会损坏已有文件
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

### 增量更新

`-o` 指向的输出目录下若已有上一次成功更新留下的本地存档（`.rman/` 目录，见下文），再次运行会自动按增量模式工作，无需任何额外参数；也可以显式指定旧清单路径。

```bash
# 增量更新：自动从本地存档发现旧版本，只下载真正变化的内容
./manifest-cli new.manifest -o ./output

# 显式指定旧清单（忽略本地存档）
./manifest-cli new.manifest -o ./output -update old.manifest

# 只校验本地文件完整性，不下载、不写盘；退出码非零表示存在待修复文件
./manifest-cli game.manifest -o ./output -verify-only

# 修复模式：逐文件重新校验，补齐本地缺失/损坏的内容（不做文件级跳过）
./manifest-cli game.manifest -o ./output -repair

# 强制全量：跳过一切校验，把匹配文件当作全新内容整体下载
./manifest-cli game.manifest -o ./output -no-verify

# 保留旧清单中已不存在的文件，以及重命名文件遗留的旧路径副本，不做默认的清理
./manifest-cli new.manifest -o ./output -keep-removed
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
| `-retry-wait` | 重试指数退避基础等待，第 N 次重试前等待 base×2^(N-1)，单次封顶 60s 与 base 中较大者 | `1s` |
| `-update` | 旧清单路径，用于增量更新比对（缺省时自动从本地存档发现） | - |
| `-repair` | 修复模式：逐文件重新校验并补齐本地缺失/损坏的内容，不做文件级跳过 | `false` |
| `-verify-only` | 仅校验本地文件完整性，不下载、不写盘 | `false` |
| `-no-verify` | 跳过校验，将全部匹配文件当作全新内容整体下载 | `false` |
| `-keep-removed` | 保留旧清单中已不存在的文件，以及重命名文件的旧路径副本，不清理磁盘（默认清理） | `false` |

`-repair`、`-verify-only`、`-no-verify` 两两互斥，同时指定多个会报错退出。

### 暂态 404 / CDN 冷对象

个别 CDN（实测如腾讯 `cdn.val.qq.com`）对长期无人访问的历史 bundle 会出现暂态 404 或高并发限流断连：对象本身存在，但边缘节点缓存已淘汰，回源暖起来需要几十秒。默认的重试窗口（1s/2s/4s）对这种场景太短，可以拉长重试节奏并降低并发：

```bash
manifest-cli old.manifest -o ./output -retry 5 -retry-wait 4s -w 8
```

`-retry 5 -retry-wait 4s` 的等待序列为 4s/8s/16s/32s/60s，总计约 2 分钟，足以覆盖多数回源暖化；`-w` 降低并发可减少限流导致的连接强制断开（`connection forcibly closed`）。

## 更新模式

无论是否携带旧清单，下载、修复、更新走的都是同一条管线，没有单独的"更新"子命令：

| 本地状态 | 行为 |
|---|---|
| 目标文件在本地不存在 | 全量下载 |
| 目标文件在本地已存在 | 按新清单的 chunk 布局逐块校验，仅补齐缺失或损坏的部分 |
| 存在旧清单（`-update` 显式指定，或从 `.rman/installed.json` 自动发现） | 额外做一层文件级跳过：路径与 chunk 序列都未变化的文件整个跳过，不做任何 IO |

四种模式（互不重叠的 CLI flag，行为差异如下）：

| 模式 | 文件级跳过 | 本地内容校验 | 下载/写盘 | 适用场景 |
|---|---|---|---|---|
| 默认（不加 flag） | 有旧清单时启用 | 仅对未被跳过的文件做 chunk 级校验补洞 | 只有变化的内容才下载写盘 | 日常增量更新 |
| `-repair` | 关闭 | 全部匹配文件逐一做 chunk 级校验补洞 | 仅缺失/损坏的部分下载写盘 | 怀疑本地文件损坏，包括被默认模式跳过的未变化文件 |
| `-verify-only` | 关闭 | 全部匹配文件逐一做 chunk 级校验 | 不下载、不写盘（dry-run） | 只检查本地完整性；退出码非零表示存在待修复文件 |
| `-no-verify` | 关闭 | 不校验 | 全部匹配文件当作全新内容整体下载 | 强制全量重新下载，跳过校验开销 |

## 本地状态目录（.rman/）

每次更新成功后，程序会在输出目录下维护一个 `.rman/` 状态目录，作为下一次增量更新的比对基准：

```
<output>/.rman/
├── installed.json                       # 当前安装状态
└── manifests/
    └── <ManifestID:016X>.manifest       # 清单原始字节存档（保留当前 + 上一份）
```

`installed.json` 示例：

```json
{
  "schema": 1,
  "manifest_id": "037EC59D5BD7C5D3",
  "manifest_file": "manifests/037EC59D5BD7C5D3.manifest",
  "source": "https://lol.secure.dyn.riotcdn.net/channels/public/releases/037EC59D5BD7C5D3.manifest",
  "updated_at": "2026-07-18T12:00:00Z"
}
```

该文件格式（含字段名、`manifests/` 相对路径中的正斜杠、`updated_at` 的 UTC RFC3339 时间格式）与姊妹 Python 项目共享，属于跨语言数据契约，不应手工修改；`schema` 用于标识格式版本，无法识别的 `schema` 会被当作没有可用状态处理，退化为全量验证——逐文件按 chunk 级重新校验，本地已完好的数据仍会被复用，只补齐缺失或损坏的部分，并不等同于 `-no-verify` 那种整体重新下载。只有整批更新全部成功时才会推进这份状态；中途失败不会写入半新不旧的版本记录。

### 临时文件与磁盘占用

每个目标文件在写入前都会先落地为同目录下的 `<文件名>.rman-tmp` 临时文件，待该文件全部内容就绪后再原子替换正式文件；替换完成前，旧文件始终保持完整、可读。但本次更新涉及的所有文件（新增、内容变化、重命名）都会先各自落地 staging，再统一发起一次批量下载、最后统一提交，因此一次更新过程中额外占用的磁盘空间峰值，约等于**本次更新涉及的全部文件的新内容大小之和**，而不是其中单个最大文件的大小。如果磁盘空间紧张，可以用 `-p` / `-f` 过滤把一次大更新拆成多批执行，降低单批峰值占用。注意：任意一批全部成功后，本地版本状态（`installed.json`）就会指向完整的新清单，后续批次会被差异比对判为"无变化"而跳过——因此从第二批起应改用 `-repair`（逐文件校验补洞，不做文件级跳过）完成剩余内容，最后可不带过滤执行一次 `-verify-only` 复核完整性。

## 架构

```
cmd/manifest-cli/     CLI 入口
pkg/rman/             RMAN 解析器（FlatBuffers + ZSTD）
pkg/core/             调度核心（Filter → Map → Schedule → Download）
pkg/diff/             新旧清单差分（ChunkID 序列判同）
pkg/update/           增量更新编排、本地 chunk 校验、清单存档与 installed.json
internal/zstream/     ZSTD 解压 + 哈希校验
internal/netpool/     HTTP Range 客户端
internal/fswriter/    LRU 文件句柄池、staging 原子替换
```

## 依赖

| 包 | 用途 |
|----|------|
| `github.com/klauspost/compress/zstd` | ZSTD 解压（纯 Go） |
| `lukechampine.com/blake3` | Blake3 哈希（纯 Go） |

## 许可证

MIT
