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
- **CDN Range 兼容**：兼容 CloudFront 非标准 multipart boundary；只返回部分 `206` 时自动顺序补齐缺失段
- **哈希校验**：支持 SHA256、SHA512、HKDF、Blake3 四种校验算法
- **下载与安装分离**：默认单独下载不维护状态；显式 `-install` 维护精确文件覆盖并执行安全增量部署
- **受管理增量**：快速跳过/移动要求状态成员通过普通文件与大小门禁；清理也只触及状态明确记录的路径
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

默认是无状态的单独下载：每次命令只处理本次匹配的文件，不读取或写入 `.rman/`，也不清理其他文件。使用同一输出目录分多次下载不同文件时，各次命令相互独立。

```bash
# 下载匹配的 DLL 文件（使用默认 CDN）
./manifest-cli game.manifest -p "\.dll" -o ./output

# 指定 CDN + 保存日志
./manifest-cli game.manifest -p "\.dll" -o ./output -u https://cdn.example.com/bundles -log download.log
```

### 受管理安装

需要把输出目录当作一套可持续更新的部署时，显式使用 `-install`。程序只在受管理安装整批成功后维护 `.rman/installed.json`，并记录实际确认落盘的文件；多次带不同筛选条件执行会累积这些覆盖。

```bash
# 首次受管理安装；筛选后的实际文件会记录到状态
./manifest-cli game.manifest -p "description\.json" -o ./game -install

# 同一清单继续安装其他文件，覆盖会累积
./manifest-cli game.manifest -p "\.wad\.client$" -o ./game -install

# 新版本增量安装：自动发现旧版本
./manifest-cli new.manifest -o ./game -install

# 显式提供旧清单作为 diff 提示（不能替代 installed.json 的所有权记录）
./manifest-cli new.manifest -o ./game -install -update old.manifest

# 只校验本地文件完整性，不下载、不写盘；退出码非零表示存在待修复文件
./manifest-cli game.manifest -o ./game -verify-only

# 修复受管理安装：逐文件重新校验，补齐本地缺失/损坏的内容
./manifest-cli game.manifest -o ./game -install -repair

# 强制全量：跳过一切校验，把匹配文件当作全新内容整体下载
./manifest-cli game.manifest -o ./game -install -no-verify

# 保留旧清单中已不存在的受管理文件，不做默认清理
./manifest-cli new.manifest -o ./game -install -keep-removed
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
| `-install` | 启用受管理安装，维护 `.rman` 状态与受授权的增量部署/清理 | `false` |
| `-update` | 旧清单路径，仅作为受管理安装的 diff 提示（需 `-install`） | - |
| `-repair` | 修复模式：逐文件重新校验并补齐本地缺失/损坏的内容，不做文件级跳过 | `false` |
| `-verify-only` | 仅校验本地文件完整性，不下载、不写盘 | `false` |
| `-no-verify` | 跳过校验，将全部匹配文件当作全新内容整体下载 | `false` |
| `-keep-removed` | 保留旧清单中已不存在的受管理文件（需 `-install`） | `false` |

`-repair`、`-verify-only`、`-no-verify` 两两互斥，同时指定多个会报错退出。
`-update` 与 `-keep-removed` 是 install-only 参数；默认单独下载使用它们会报错。

如果输出目录已经存在 `.rman/installed.json`，默认单独下载、`-repair` 或 `-no-verify` 会拒绝写入，避免绕过安装状态造成混合版本；请改用 `-install` 或另选输出目录。只读的 `-verify-only` 仍然允许。

### 暂态 404 / CDN 冷对象

个别 CDN（实测如腾讯 `cdn.val.qq.com`）对长期无人访问的历史 bundle 会出现暂态 404 或高并发限流断连：对象本身存在，但边缘节点缓存已淘汰，回源暖起来需要几十秒。默认的重试窗口（1s/2s/4s）对这种场景太短，可以拉长重试节奏并降低并发：

```bash
manifest-cli old.manifest -o ./output -retry 5 -retry-wait 4s -w 8
```

`-retry 5 -retry-wait 4s` 的等待序列为 4s/8s/16s/32s/60s，总计约 2 分钟，足以覆盖多数回源暖化；`-w` 降低并发可减少限流导致的连接强制断开（`connection forcibly closed`）。

### 多 Range 兼容处理

部分 Riot CDN 节点返回 `multipart/byteranges` 时，CloudFront boundary 含冒号却
未按 MIME 规范加引号。客户端会兼容提取该 boundary，并依据每段
`Content-Range` 映射响应，不依赖 multipart 返回顺序。

如果 CDN 确实只满足多 Range 请求的一部分，客户端会复用已返回的数据，并在现有
Worker 内顺序请求缺失段；后续 Bundle 直接使用单 Range 请求，避免重复探测。
每段响应仍需通过范围、长度、ZSTD 解压及 Chunk 哈希校验，不会把一段内容错误
映射到多个请求范围。

## 操作与校验策略

操作意图与校验策略是两个独立维度：

| 操作 | 状态读写 | manifest diff | 移动/清理权限 | 默认用途 |
|---|---|---|---|---|
| 默认单独下载 | 不读不写 `.rman` | 不使用 | 无 | 独立获取一个或一组文件 |
| `-install` 受管理安装 | 整批成功后写 schema 2 | 自动发现或 `-update` | 仅限状态已记录文件 | 部署并持续更新一个目录 |

| 校验策略 | 文件级快速跳过 | 本地内容校验 | 下载/写盘 |
|---|---|---|---|
| 默认 AUTO | 仅受管理安装中，对状态成员且普通文件/大小匹配的 unchanged 目标启用 | 其余目标逐 chunk 校验补洞 | 只下载缺失/损坏内容 |
| `-repair` | 关闭 | 全部匹配文件逐 chunk 校验补洞 | 只下载缺失/损坏内容 |
| `-verify-only` | 关闭 | 全部匹配文件逐 chunk 校验 | 不下载、不写盘、不改状态 |
| `-no-verify` | 关闭 | 不校验 | 全部匹配文件整体下载 |

受管理 AUTO 中，manifest 判定“内容未变”还不够：路径必须位于 `installed.json.files`，磁盘目标必须是普通文件且大小符合清单，才能快速跳过。文件缺失、类型异常、大小不同或未在状态中记录时，都会降级为逐 chunk 校验/下载。

## 本地状态目录（.rman/）

只有 `-install` 受管理安装会维护 `.rman/`。`installed.json` 同时记录清单指针和当前清单下实际确认的受管理文件覆盖：

```
<output>/.rman/
├── installed.json                       # 当前安装状态
└── manifests/
    └── <ManifestID:016X>.manifest       # 清单原始字节存档（保留当前 + 上一份）
```

`installed.json` 示例：

```json
{
  "schema": 2,
  "manifest_id": "037EC59D5BD7C5D3",
  "manifest_file": "manifests/037EC59D5BD7C5D3.manifest",
  "source": "https://lol.secure.dyn.riotcdn.net/channels/public/releases/037EC59D5BD7C5D3.manifest",
  "updated_at": "2026-07-29T12:00:00Z",
  "files": [
    "Config/description.json",
    "DATA/FINAL/Maps/Shipping.wad.client"
  ]
}
```

`files` 是受管理所有权边界，不是“本轮发生过网络下载”的历史：被当前运行确认完整或成功提交的目标都会记录；同一清单的多次部分安装会累积覆盖。跨版本时，只携带内容未变、仍由新清单声明且通过普通文件/大小检查的旧覆盖；变化但本轮未选择的文件不会冒充已安装。

该格式与姊妹 Python 项目共享，路径统一使用 `/`、排序并去重，`updated_at` 使用 UTC RFC3339；schema 2 必须显式包含数组类型的 `files`（空覆盖写作 `[]`，字段缺失或 `null` 均无效）。legacy schema 1 仍可提供旧 manifest 指针作为 diff 提示，但没有可信文件覆盖，不能授权 SKIP、MOVE 或 REMOVE；下一次成功受管理安装会写成 schema 2。未知 schema 按无状态处理。

只有本轮全部目标和受管理清理均成功时才会推进状态；中途失败会保留上一份 `installed.json`。`-keep-removed` 会保留磁盘旧文件，但这些已不属于新清单的路径仍会从新状态覆盖中移除。

### 临时文件与磁盘占用

每个目标文件在写入前都会先落地为同目录下的 `<文件名>.rman-tmp` 临时文件，待该文件全部内容就绪后再原子替换正式文件；替换完成前，旧文件始终保持完整、可读。但本次操作涉及的所有文件会先各自落地 staging，再统一发起一次批量下载、最后统一提交，因此额外磁盘空间峰值约等于**本次操作涉及的全部文件的新内容大小之和**。磁盘空间紧张时可用 `-p` / `-f` 把受管理安装拆成多批；schema 2 会记录并累积每批实际确认的文件，不再要求后续批次改用 `-repair`。

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
