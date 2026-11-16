# SubtitleForge 智能字幕工厂

自动为影视文件生成中文字幕的全流程工具：语音转录 → 转录质检 → AI 翻译 → 翻译校对 → 大模型润色，最终输出自然地道的中文字幕（`.zh.srt` / `.zh.rs.srt`）。支持监控目录自动入队、Jellyfin/Emby 媒体入库联动、Web 端实时进度管理。

**当前版本：v1.1.1** · [更新日志](CHANGELOG.md) · [发布指引](RELEASE.md)

## v1.1.1 更新功能

| 类别 | 内容 |
| --- | --- |
| 增强流程放行 | 由「增强转录」发起的任务，质检未通过也不再判失败：字幕直接保留在视频原目录，不归档到「质检未通过字幕目录」 |
| 翻译质量放行 | 翻译质检未通过时同样放行落盘并继续润色，任务以 `done` 收尾，不再因质检分数中断交付 |
| 假译文保护 | 翻译未生效（≥30% 条目仍为原文）时落盘译文后直接置 `done`，跳过校对与润色，避免对无效内容二次改写 |
| 原因可观测 | 放行原因与分数写入 `enhanced_note`，任务卡片以警告行展示；字幕记录带 `enhanced_released` 标记 |
| 行为隔离 | 普通重试、扫描入队、只转录等非增强流程不受影响，质检未通过仍判失败并按设置归档 |

## v1.1.0 更新功能

| 类别 | 内容 |
| --- | --- |
| 增强转录 | 失败任务多出「增强重试」：忽略已有转录结果，改用更强的 ASR 模型（默认 `large-v3`）重新转录视频，只对本次重试生效 |
| 批量增强重试 | 队列顶部新增「增强重试」按钮，一键用增强模型重跑全部失败任务 |
| 完成页操作 | 已完成任务卡片提供「增强重试」（更强模型重转并覆盖原字幕）与「删除字幕」（删除本任务产出的字幕文件，任务记录保留） |
| 任务列表分页 | 队列与字幕页支持每页 10/20/50/100 条切换与上一页/下一页，默认 20 条并记住设置 |
| 任务搜索 | 队列上方搜索框可按片名/路径搜索全部任务，结果分页展示 |
| 模型可选 | 设置页新增增强转录开关、增强模型（large-v3 / large-v2 / medium …）与独立精度，不影响全局默认 ASR 模型 |
| 强制重转 | 增强重试会跳过「目标字幕已存在」与「复用上次转录结果」两条短路逻辑，确保真正重跑 |
| 显存守护 | 转录前按增强模型的真实显存需求估算并腾挪（large-v3 float16 约 3.9G），避免 8G 卡直接 OOM |
| 精确卸载 | ASR 释放时按本次实际加载的模型卸载，修复 llama 托管模式下增强模型残留显存 |
| 可观测 | 任务列表展示该任务实际使用的 ASR 模型名 |

## v1.0.1 更新功能

| 类别 | 内容 |
| --- | --- |
| 同批次限流 | 新增 `BATCH_SIZE`（1–100，默认 20）：扫描/批量添加的任务先进「等候队列」，当前批次全部到达终态（含失败）后才放行下一批，流水线/并行/两阶段模式统一受限 |
| 批次内环转 | 同一任务的下游环节（转录完成转入翻译等）视为同批次重投，不占用新名额，也不会被限流卡住 |
| 放行去抖 | 批量扫描时先攒满等候队列再统一放行，避免逐个投递造成批次抖动 |
| 队列视图 | 前端「全部排队」改为「等候队列」，当前批次成员与等候数量分开展示，不再被上千条任务刷屏 |
| LLM 超时治理 | 新增 `LLM_TIMEOUT`（默认 180s）、`LLM_MAX_ATTEMPTS`（默认 3）、`LLM_TOTAL_BUDGET`（默认 360s），单次调用受总预算约束 |
| 假死自愈 | 请求超时视为上游进程假死，首次超时即卸载该模型触发 llama-swap 重建，不再空等整条流水线 |
| 配置校验 | 设置写接口改为白名单校验，`batch_size` 服务端夹紧到 1–100，脏值不再入库 |

## v1.0.0 版本要点

| 类别 | 内容 |
| --- | --- |
| 润色模型解耦 | 新增润色专用 API 地址 / Key / 模型三项独立配置，不再强制复用翻译或校对模型 |
| 模型回退链 | `润色模型 → 校对模型 → 翻译模型`，任一项留空自动向下回退，配置为空也能跑通 |
| 质检建议驱动润色 | 翻译质检未通过的条目，其问题与修改建议会注入润色 prompt，按意见定向修正，而非泛泛润色 |
| 润色可观测 | 任务详情展示实际使用的润色模型与吞吐（tok/s），已通过质检的字幕不再堆叠无效建议 |
| 手机端下拉修复 | 模型选择由 `input + datalist` 改为原生 `select`，修复移动端只显示单个模型的过滤问题，支持留空回退与手动输入 |
| ASR 双引擎 | 新增 `ASR_PROVIDER=llama`：由 llama-swap 托管 whisper.cpp（ggml），可在 Web 端一键部署模型并自动改写配置、重启服务 |
| 显存感知调度 | 按 GPU 显存总量决定单模型串行或多模型共存（`MULTI_MODEL_VRAM_MB`），阶段切换前自动卸载无关模型 |
| 扫描分段执行 | 支持“只补转录”与“只补翻译”两类扫描，避免重复跑全流程 |
| 质检归档 | 未通过校验的字幕可按开关归档到指定目录，便于集中复核 |
| 任务控制 | 支持一键清空失败任务、暂停/恢复全部队列、任务级中断检查 |
| 性能优化 | 数据库连接复用 + WAL、设置项内存缓存、任务统计单次聚合、进度事件节流、LLM 连接池复用、全量扫描去 N+1 查询 |

完整对比见 [CHANGELOG.md](CHANGELOG.md)。

## 功能特性

- **全自动流水线**：放入监控目录即自动处理，支持「流水线」与「两阶段」两种模式
- **双层质检**：转录质检 + 翻译校对，不合格自动标记复核，并可归档
- **大模型润色**：校对通过后用 LLM 润色为自然中文，输出与译文并存的 `.zh.rs.srt`
- **智能队列**：自动去重、字幕缺失优先、按 GPU 显存自动推荐并发
- **Web 管理界面**：实时进度/速度展示、任务增删改查、质检报告展开、批量操作、手机端适配
- **媒体联动**：Jellyfin / Emby / Plex 媒体入库自动触发字幕生产（Webhook）
- **资源监控**：CPU / 内存 / GPU / Token 吞吐实时曲线
- **模型自由**：ASR 支持 faster-whisper 全系列与 whisper.cpp；LLM 对接任意 OpenAI 兼容接口

## 工作流程

```
监控目录 / Webhook 入库
        ↓
  提取音频 (ffmpeg)
        ↓
  语音转录 (faster-whisper / whisper.cpp)
        ↓
  转录质检 (自动评分)
        ↓
  AI 翻译 (LLM, 如 Sakura-7B)
        ↓
  翻译校对 (LLM 复核, 输出问题与建议)
        ↓
  大模型润色 (可选, 建议注入, 输出 .zh.rs.srt)
```

## 依赖说明

### 1. 系统依赖

| 依赖 | 版本 / 要求 | 说明 |
| --- | --- | --- |
| Docker | 20.10+ | 容器部署 |
| Docker Compose | v2 | `${VAR:-默认值}` 语法、`gpus: all` 需要 v2 |
| Linux x86_64 | — | 镜像仅构建 `linux/amd64` |
| ffmpeg | 4.x+ | 音频抽取；**镜像内已安装**，裸机运行需自行安装 |
| NVIDIA 驱动 | 535+ | 仅 GPU 推理需要 |
| NVIDIA Container Toolkit | 最新 | 仅 GPU 推理需要，提供 `gpus: all` 能力 |

### 2. Python 依赖（`requirements.txt`，镜像内自动安装）

| 包 | 用途 |
| --- | --- |
| `fastapi` / `uvicorn` | Web 服务与 API |
| `watchdog` | 监控目录文件事件 |
| `apscheduler` | 定时扫描调度 |
| `psutil` | CPU / 内存指标采集 |
| `faster-whisper` | 本地语音识别（CTranslate2 后端） |
| `nvidia-cublas-cu12` / `nvidia-cudnn-cu12` | faster-whisper GPU 推理所需 CUDA 运行库 |
| `requests` | LLM / 模型下载 HTTP 调用 |
| `pydantic` / `python-multipart` | 请求校验与表单上传 |

运行环境：Python 3.11（镜像基础 `python:3.11-slim`）。

### 3. 模型依赖

| 模型 | 是否必需 | 获取方式 |
| --- | --- | --- |
| ASR 模型（faster-whisper `tiny/base/small/medium/large-v3`） | 二选一 | 首次运行自动从 HuggingFace 下载，可用 `HF_ENDPOINT` 换镜像 |
| ASR 模型（whisper.cpp ggml，`whisper-small` / `whisper-large-v3-turbo`） | 二选一 | `ASR_PROVIDER=llama` 时在 Web 端「一键部署」，下载到 llama-swap 模型目录 |
| 翻译 / 校对 / 润色 LLM | 必需（翻译阶段） | 自建或第三方 OpenAI 兼容服务：llama.cpp、llama-swap、Ollama、vLLM、OpenAI 等均可 |

> 项目不内置任何 LLM 权重，也不绑定厂商；只要提供 `/v1/chat/completions` 接口即可接入。

### 4. 可选依赖

| 组件 | 用途 |
| --- | --- |
| llama-swap 容器 + `config.yaml` | `ASR_PROVIDER=llama` 时托管 whisper.cpp，并统一管理 LLM 模型切换 |
| 宿主机 `/var/run/docker.sock` | 容器内重启 llama-swap / 部署模型时需要 |
| Jellyfin / Emby / Plex | 通过 Webhook 触发入库自动生产 |

### 5. 硬件建议

| 配置项 | 最低 | 推荐 |
| --- | --- | --- |
| CPU | 4 核 | 8 核以上 |
| 内存 | 8 GB | 16 GB 以上 |
| GPU | NVIDIA 4 GB（纯转录可无 GPU） | NVIDIA 8 GB 以上 |
| 存储 | 20 GB（模型 + 缓存） | 100 GB 以上 |

显存参考：7–8B 量化模型约 6.8 GB，faster-whisper small int8 约 1.1 GB；20 GB 以下显存建议单模型串行，35B-A3B 级模型需约 21 GB 权重空间，显存不足会直接 502。

## 快速开始

### 方式一：使用预构建镜像

```bash
docker pull ghcr.io/lzxssg/subtitle-forge:latest
```

`docker-compose.yml` 中注释掉 `build: .`，改用：

```yaml
image: ghcr.io/lzxssg/subtitle-forge:latest
```

### 方式二：源码构建

```bash
git clone https://github.com/lzxssg/subtitle-forge.git
cd subtitle-forge
cp .env.example .env      # 按需修改
docker compose up -d --build
```

打开 `http://<服务器IP>:8310`。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WEB_PORT` | `8310` | Web 服务端口 |
| `MODE` | `manual` | 运行模式：manual / scheduled / guard |
| `QUEUE_PAUSED` | `false` | 启动时是否暂停队列 |
| `WATCH_DIRS` | `/Media` | 监控目录（容器内路径，逗号分隔） |
| `SCAN_INTERVAL_MINUTES` | `60` | 自动扫描间隔 |
| `TASK_WORKERS` | `1` | 任务线程数 |
| `BATCH_SIZE` | `20` | 同批次任务数量上限（1–100），整批结束才放行下一批 |
| `SMART_QUEUE` | `true` | 智能队列（去重 + 优先级 + 限流） |
| `MULTI_MODEL_VRAM_MB` | `16000` | 多模型共存所需显存阈值 |
| `HF_ENDPOINT` | `https://hf-mirror.com` | HuggingFace 镜像 |
| `ASR_PROVIDER` | `local` | `local` faster-whisper / `llama` llama-swap 托管 |
| `ASR_MODEL` | `small` | tiny / base / small / medium / large-v3 |
| `ASR_DEVICE` | `auto` | auto / cuda / cpu |
| `ASR_COMPUTE_TYPE` | `int8` | int8 / float16 / float32 |
| `ASR_LANGUAGE` | `auto` | 源语言 |
| `ASR_CPU_THREADS` | `8` | CPU 线程数 |
| `ENHANCED_TRANSCRIBE_ENABLED` | `true` | 失败任务是否允许「增强重试」 |
| `ENHANCED_ASR_MODEL` | `large-v3` | 增强重试使用的 ASR 模型 |
| `ENHANCED_ASR_COMPUTE_TYPE` | 空 | 增强重试精度，留空沿用 `ASR_COMPUTE_TYPE` |
| `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL` | 空 / 空 / `qwen2.5:7b` | 翻译模型 |
| `LLM_TEMPERATURE` | `0.3` | 翻译采样温度 |
| `LLM_TIMEOUT` | `180` | 单次 LLM 请求超时（秒） |
| `LLM_MAX_ATTEMPTS` | `3` | 单次 LLM 调用重试次数 |
| `LLM_TOTAL_BUDGET` | `360` | 单次 LLM 调用总时间预算（秒） |
| `REVIEW_BASE_URL` / `REVIEW_API_KEY` / `REVIEW_MODEL` | 空 | 校对模型，留空回退翻译模型 |
| `REVIEW_SAMPLE_COUNT` | `12` | 校对抽样条数 |
| `POLISH_ENABLED` | `false` | 是否启用润色阶段 |
| `POLISH_BASE_URL` / `POLISH_API_KEY` / `POLISH_MODEL` | 空 | 润色模型，留空依次回退校对、翻译模型 |
| `POLISH_TEMPERATURE` | `0.3` | 润色采样温度 |
| `PHASE_MODE` | `pipeline` | pipeline 流水线 / staged 两阶段 |
| `SOURCE_LANG` / `TARGET_LANG` | `auto` / `zh` | 语言设置 |
| `TRANSLATE_ONLY` | `false` | 仅翻译已有字幕 |
| `SKIP_EXISTING_SUBTITLE` | `true` | 已有字幕跳过 |
| `SUBTITLE_SUFFIX` | `.zh` | 字幕后缀 |
| `TRANSCRIPT_QUALITY_THRESHOLD` | `60` | 转录质检阈值 |
| `TRANSLATION_QUALITY_THRESHOLD` | `70` | 翻译质检阈值 |
| `LLAMA_MODELS_HOST` | 空 | 宿主模型目录，映射到容器 `/models-llama` |
| `LLAMA_SWAP_CONFIG` | `/config-llama.yaml` | llama-swap 配置文件路径 |
| `LLAMA_SWAP_CONTAINER` | `llama-swap` | llama-swap 容器名 |
| `WEBHOOK_ENABLED` | `false` | 媒体联动 Webhook 开关 |
| `WEBHOOK_SECRET` | 空 | Webhook 共享密钥（留空不校验） |

## 媒体联动（Webhook）

1. 设置 → 媒体联动开启 Webhook，可选填共享密钥
2. 填写路径映射（播放器路径与字幕工厂路径不一致时）
3. 媒体服务器 Webhook 指向：

```
http://<字幕工厂IP>:8310/api/webhook/jellyfin?token=<你的密钥>
```

## 常见问题

**Q：8G 显存能跑吗？** 能。ASR 用 small/medium int8，翻译/润色用 7–8B 量化模型，或切两阶段模式先转录后翻译。

**Q：润色没生效？** 检查 `POLISH_ENABLED`，以及润色模型是否可达；设置页可单独“测试”润色接口，留空模型会按回退链取校对模型。

**Q：翻译质量不好怎么办？** 日语建议 Sakura-7B 系列；开启润色并让质检建议参与，可显著提升中文自然度。

**Q：模型从哪里下载？** ASR 模型首次运行自动从 HuggingFace（默认 hf-mirror）下载；LLM 由自建服务提供，填入 API 地址即可。

## License

[MIT](LICENSE)
