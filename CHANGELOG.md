# Changelog

本项目的版本变更记录。版本号遵循 `MAJOR.MINOR.PATCH`。

## v1.1.5（2026-11-10）

修复版本：ASR 运行方式切到「llama-swap 托管」时误报「llama 托管暂无已部署模型，请先下载并部署」。

### 问题

- 设置页初始化（`loadConfigForm`）只渲染 ASR 托管状态，没有先拉取 `/api/models/list`，导致 `window._llamaManaged` 为空数组。
- 此时把 ASR 运行方式切到 `llama`，前端判定所有模型都未部署：选项全部被禁用并标记「（未部署）」、ASR 模型被强制改选，最终弹出「暂无已部署模型」。
- 后端判定实际正常：`/api/models/list` 返回 `llama_managed=['small','large-v3-turbo']`、`llama_swap_ready=true`，`config.yaml` 与 ggml 模型文件均齐备。

### 修复

- `loadConfigForm` 在设置 `asr_provider` 之前先 `await refreshAsrModels()`，保证 `_llamaManaged` 已就绪；异常不影响其余配置加载。
- `APP_VERSION` 与 `VERSION` 同步为 `1.1.5`，镜像 tag 与内部版本一致。

## v1.1.4（2026-11-09）

维护版本：引入音频缓存自动清理，解决 `data/audio/task_*.wav` 无界增长（实测 5 天累积 150 GiB、日均 ≈30 GiB，占满存储空间 1）。

### 自动清理（核心）

- **任务完成即删**：任务进入 `done` 后由队列输出钩子立即删除对应 `task_{id}.wav`（`AUDIO_CACHE_CLEANUP_ON_DONE`，默认 `true`）。
- **定期兜底巡检**：新增 `audio-cache-cleaner` 线程，启动 45s 后首扫历史积压，此后每 30 分钟巡检一次。
  - `done` 任务：直接清理；
  - `failed` / `cancelled` 任务：超过 `AUDIO_CACHE_RETENTION_HOURS`（默认 24h）后清理，保留排查与重试窗口；
  - 无任务记录的孤儿缓存：超过保留期后清理；
  - 运行中任务（`_running_tasks`）一律跳过，绝不打断正在转写的任务。
- **手动清理接口增强**：`POST /api/cache/clear` 现在跳过运行中任务，并返回 `removed` / `freed` / `skipped_running`。
- **新增状态接口**：`GET /api/cache/status` 返回缓存文件数、总字节、清理开关、保留时长与上次巡检结果。
- 清理失败（文件占用/权限）仅告警，不影响任务主流程。

- **版本接口**：新增 `GET /api/version` 返回 `config.APP_VERSION`；页面顶部版本号改为运行时从接口拉取，不再硬编码在 `static/index.html`，避免界面版本与镜像版本漂移。

### 配置项

| 键 | 环境变量 | 默认 |
|---|---|---|
| `audio_cache_cleanup_enabled` | `AUDIO_CACHE_CLEANUP_ENABLED` | `true` |
| `audio_cache_cleanup_on_done` | `AUDIO_CACHE_CLEANUP_ON_DONE` | `true` |
| `audio_cache_retention_hours` | `AUDIO_CACHE_RETENTION_HOURS` | `24` |

## v1.1.3（2026-11-09）

功能版本：看门狗升级为「无人值守自动修复」，不再需要人工点按钮。

### 自动修复（核心）

- 看门狗巡检周期固定 20s，判定分两类异常：
  1. **任务卡死**：worker 心跳停滞超过 `watchdog_stuck_seconds`（默认 60s）且任务仍在中间态；
  2. **队列空转**：有等待任务、无在跑任务，且完成数在 `watchdog_idle_seconds`（默认 180s）内不再增长。
- 命中任一条件即自动执行完整修复：卸载非工作模型 → 只强杀心跳停滞的 ffmpeg → 解除批次闸门死锁 → 中间态任务归位 → 补/重建工作线程 → 重新入队续跑 → 按需预热工作模型。
- 自动修复有 300s 冷却，避免反复打断在跑任务；正在跑的任务永不被打断（只杀「心跳停滞」或「不在运行集合」的提取进程）。
- 每次自动修复写入日志（`看门狗：已自动修复 #N（原因）→ 终止提取 x、归位 y、续跑 z`）并通过 SSE `queue_repair` 推送前端。

### 新增设置项（设置页 → 队列）

- `看门狗自动修复`（`watchdog_auto_heal`，默认开启）
- `卡死判定阈值`（`watchdog_stuck_seconds`，默认 60s，最小 15s）
- `队列空转判定`（`watchdog_idle_seconds`，默认 180s，最小 60s）

### 新增接口

- `POST /api/queue/watchdog`：手动触发一次巡检（便于验证与运维），返回本次判定结果与完整健康快照
- `GET /api/queue/repair` 的 `health` 增加 `watchdog` 段：`auto_heal`、`idle_ticks`、`auto_heal_count`、`last_auto_age_seconds`、`last_trigger`、`last_report`、`finished`

### 验证（2026-11-09）

- 逻辑链路测试（容器内直接调用真实 `_watchdog_tick`，repair 用替身）：① 心跳停滞 200s → 命中 stuck 并调用 `repair(force_unload=True, restart_workers=True)`，`auto_heal_count` 与 `last_report` 正确落地；② 空转场景第 3 次巡检（60s）命中并修复；③ 冷却期内不重复触发
- 线上巡检：`health.watchdog.last_check_age_seconds` 稳定在 20s 周期内，`stuck_workers` 恒为空，任务正常推进，无误判自动修复
- 手动触发：`POST /api/queue/watchdog` 返回 `{"ok":true,"result":{...},"health":{...}}`，空跑不产生副作用
- 故障注入：对真实 ffmpeg 进程 SIGSTOP 后心跳年龄上涨、恢复后回落；因未持续超过阈值未触发修复（符合预期，避免误伤）

### 其它

- `health` 的卡死判定改用可配置阈值（不再硬编码 900s）
- 页脚版本号 v1.1.3

## v1.1.2（2026-11-09）

修复版本：解决「队列空转、任务不推进」的提取死锁，并新增一键任务修复与强制卸载模型能力。

### 队列假死修复（根因）

- 根因：`app/pipeline/extract_audio.py` 的音频提取用 `for line in proc.stdout` 读取 ffmpeg 输出，ffmpeg 在写满管道缓冲后阻塞、Python 侧又等不到行结束，worker 线程永久卡死在读取处；队列批次已放行但无 worker 领取，表现为「进行中 0、等待中数百、任务全停在 pending」
- 修复：ffmpeg stdout 改由独立读线程投递（`queue.Queue`），stderr 落临时文件、进程结束后统一输出；主线程按 `proc.poll()` 轮询退出码
- 新增双超时保护：空闲超时（默认 120s 无任何输出）与总时长超时（按时长估算），超时即 `proc.kill()` 并抛错退出，不再无限等待
- 新增取消回调：任务被取消/暂停时立即终止对应 ffmpeg 进程
- 新增进程登记表 `ACTIVE_PROCS` 与强制终止接口 `kill_task` / `kill_all` / `kill_tasks` / `active_task_ids`

### 队列看门狗

- worker 心跳：每个 worker 记录当前任务、阶段与最后心跳时间（`_worker_state` / `_last_beat`），每 1.5s 刷新
- 看门狗线程每 30s 巡检：worker 心跳停滞超过阈值（默认 900s）判定卡死并记录告警，可由修复接口一键恢复
- `_run_loop` 增加异常兜底，单任务异常不再导致 worker 线程静默退出

### 一键任务修复

- 接口：新增 `POST /api/queue/repair`（执行修复并返回快照）、`GET /api/queue/repair`（只读查看当前健康状态）
- 修复动作：① 强制卸载 llama-swap 闲置模型，把显存让给工作模型（翻译/润色在跑时只卸载非工作模型，保留 `sakura-gpu` / `glm4_9b`）；② 强杀心跳停滞的 ffmpeg（正常在跑的不动）；③ 解除批次闸门死锁、清空等候队列并重新放行；④ 中间态任务（extracting/transcribing/checking/paused）归位为 pending 重排；⑤ 重建或补充工作线程（有任务在跑时只补线程、不打断）；⑥ 预热目标模型后立即放行新批次
- `POST /api/queue/pause` 恢复队列时支持 `force_unload`，恢复前先卸载闲置模型
- 前端：工具栏新增「任务修复」按钮，二次确认后调用修复接口并以 toast/日志展示卸载模型、终止进程、归位任务、当前批次等明细；恢复队列时自动携带强制卸载参数

### 其他

- 页脚版本号更新为 v1.1.2

## v1.1.1（2026-11-06）

修订版本：增强转录流程产出的字幕不再被视为失败，直接保留在视频原目录。

### 增强流程放行

- 任务 meta 新增 `enhanced_flow`：增强重试时置位并保留到任务结束，用于判定本任务是否走增强流程
- 转录质检未通过：增强流程下不再判 `failed`、不再归档到「质检未通过字幕目录」，`.transcript.srt` 留在视频同目录，任务继续翻译
- 翻译质检未通过：增强流程下同样放行，译文留在视频原目录，任务继续润色并置为 `done`
- 翻译未生效（≥30% 条目仍为原文）：增强流程下落盘译文后直接置 `done`，跳过后继校对与润色，避免二次改写
- 放行结果写入 `enhanced_note`，任务列表以警告行展示分数与原因；字幕记录标记 `enhanced_released`
- 非增强流程（普通重试、扫描入队、只转录）行为不变：质检未通过仍判失败并按设置归档
- 界面文案：队列顶部批量按钮「增强重试失败」更名为「增强重试」，与单任务按钮一致（版本号不变）
- 已完成任务卡片新增操作：`删除字幕`（删除该任务生成的转录/译文/润色字幕文件与记录，任务记录保留）、`增强重试`（用增强模型重新转录已完成任务并覆盖现有字幕）
- 接口：新增 `POST /api/tasks/{id}/subtitles/delete`；`POST /api/tasks/{id}/retry?enhanced=true` 放开 `done` 状态，普通重试仍拒绝已完成任务
- 删除字幕同时清理字幕相关 meta（`transcript_score` / `polish_*` / `enhanced_note`），避免后续重试复用旧转录结果
- 任务列表分页：支持每页 10/20/50/100 条（默认 20，浏览器记忆），提供上一页/下一页与「第 x/y 页」提示；超出范围自动回到最后一页
- 任务搜索：任务队列上方新增搜索框，按片名/路径在全部任务中检索（防抖 250ms），搜索结果同样分页展示
- 服务端分页与过滤：`GET /api/tasks` 新增 `view`/`q`/`batch` 参数，`offset`+`limit` 真正落到 SQL；当前队列与各环节队列仍只取本批次任务
- 列表统计改用 `/api/stats` 全量口径（新增 `running`/`review` 计数），避免分页后统计随当前页漂移；「最近记录」独立查询最近 20 条终态任务

## v1.1.0（2026-11-06）

功能版本：为失败的字幕任务提供「增强转录」重试，可用更强的 ASR 模型（如 `large-v3`）重新转录视频。

### 增强转录

- 单任务：失败任务操作区新增「增强重试」，默认取设置页的增强模型（`ENHANCED_ASR_MODEL`，默认 `large-v3`）
- 批量：队列顶部新增「增强重试失败」，一次为全部失败任务打上增强标记
- 接口：`POST /api/tasks/{id}/retry` 与 `POST /api/tasks/retry-failed` 新增 `enhanced` / `model` 查询参数，可临时指定模型
- 一次性生效：模型覆盖写入任务 meta（`asr_model_override` / `asr_compute_override` / `force_retranscribe`），转录前取出并清除，不改变全局 `ASR_MODEL`
- 强制重转：增强重试跳过「目标字幕已存在则跳过」与「复用上次转录结果」两条短路逻辑，确保真正重新转录
- 设置项：新增 `ENHANCED_TRANSCRIBE_ENABLED`（总开关）、`ENHANCED_ASR_MODEL`、`ENHANCED_ASR_COMPUTE_TYPE`（留空沿用全局精度），写入接口做白名单与字符校验

### 显存与资源

- 转录前显存守护按增强模型的真实需求估算（`estimate_asr_vram_mb` 支持按模型名+精度计算，`large-v3` float16 约 3900MB），空闲不足时先卸载常驻 LLM
- `transcribe.get_model` / `release_model` 支持按模型名缓存与卸载，增强模型与默认模型互不覆盖
- 修复 llama 托管模式下 ASR 释放只认全局模型的问题：改为按本次实际使用的模型精确卸载

### 界面

- 任务列表展示该任务实际使用的 ASR 模型名，便于确认增强转录是否生效

## v1.0.1（2026-11-05）

补丁版本：解决大批量入队时任务一次性全量灌入导致的显存争抢与页面刷屏，以及上游 LLM 假死造成队列长时间无进展的问题。

### 同批次任务数量限制

- 新增设置项 `batch_size`（1–100，默认 20），Web 设置页可直接调整
- 扫描 / 批量添加产生的任务先进入「等候队列」，仅当当前批次成员全部到达终态（done / failed / cancelled / skipped / paused）后，才按上限放行下一批
- 流水线、并行、两阶段三种模式统一受限；同一任务的下游环节（如转录完成转入翻译）按「同批次重投」处理，不占用新名额、也不会被自己的批次挡住
- 放行去抖：批量扫描时先攒满等候队列再统一放行，避免逐个投递造成批次抖动
- 前端「全部排队」更名为「等候队列」，当前批次成员与等候数量分开统计，避免上千条任务刷屏

### LLM 调用健壮性

- 新增 `llm_timeout`（单次请求超时，默认 180s）、`llm_max_attempts`（重试次数，默认 3）、`llm_total_budget`（单次调用总时间预算，默认 360s），均可通过环境变量或设置页调整
- llama-swap 上游进程崩溃后可能进入假死：首次请求超时时自动卸载该模型以触发重建，不再干等整条流水线
- 退避等待时长受总预算约束，超预算立即抛出明确错误，避免单个任务长时间占住工作线程

### 修复与加固

- 设置写入接口改为白名单校验，非可编辑项直接跳过
- `batch_size` 在服务端夹紧到 1–100，非法值（空 / 非数字 / 越界）不再写入数据库

## v1.0.0（2026-11-05）

首个公开版本：从「能跑」到「可发布、可长期运行」的完整开源版。

### 核心流水线

- **音频提取**：ffmpeg 抽取 16kHz 单声道 wav，`-progress pipe:1` 实时解析进度，多线程解码（`-threads 0`）
- **语音转录**：faster-whisper（CTranslate2）与 whisper.cpp（llama-swap 托管）双引擎，`ASR_PROVIDER` 一键切换
- **转录质检**：按时长覆盖、重复率、空段比例、语种一致性、字符密度多维度评分，低于阈值标记复核
- **AI 翻译**：按 token 预算分批，携带上下文前后文，输出格式容错解析，批次失败自动拆半递归重试
- **翻译校对**：抽样复核译文，输出问题与修改建议
- **大模型润色**：可选阶段，质检建议注入 prompt 定向修正，与译文并存输出 `.zh.rs.srt`

### 队列与调度

- 智能队列：路径去重、字幕缺失优先、按时长与状态排序、失败最多自动重试 3 轮
- 进度事件 SSE 推送，任务级中断检查，暂停 / 恢复全部队列
- 三种运行模式：`manual` 手动 / `scheduled` 定时扫描 / `guard` 实时监听
- 扫描分段执行：支持「只补转录」「只补翻译」，避免重复跑全流程
- 显存感知调度：按显存阈值决定单模型串行或多模型共存；阶段切换前自动卸载无关模型（llama-swap / Ollama）
- 任务结束后自动卸载 LLM，释放显存（`llm_manager.unload_all()`）

### Web 管理界面

- 任务列表 / 详情 / 质检报告展开、批量操作、拖拽与移动端适配
- 实时进度与 xRT 速率、CPU / 内存 / GPU / Token 吞吐曲线
- 设置页全量配置项在线修改，模型接口逐个「测试」
- 模型选择在移动端改用原生 `select`，支持留空回退与手动输入

### 媒体联动

- Webhook 接入 Jellyfin / Emby / Plex 入库事件，支持共享密钥与播放器→工厂路径映射

### v1.0.0 性能优化

| 优化项 | 说明 |
| --- | --- |
| 数据库连接复用 | 线程局部连接复用（`threading.local`），开启 WAL + `synchronous=NORMAL`，避免每次调用新建连接与提交 |
| 设置项缓存 | `get_setting` 走内存缓存，写操作自动失效，消除每轮调度/扫描的重复查库 |
| 任务统计聚合 | `task_stats` 由 8 条 `COUNT` 合并为单条条件聚合查询 |
| 任务列表去 N+1 | 字幕一次批量查询（`subtitles_map`）、`meta` 直接复用行内 JSON，文件大小加 5 秒缓存 |
| 进度回调节流 | 转录逐段落回调按 0.8s / 1 个百分点节流，消除千次级重复 DB 写与 SSE 广播 |
| 转录字符统计 | 由每 0.5s 全量累加改为增量累计，去掉 O(n²) 开销 |
| LLM 连接池 | 复用 `requests.Session`（HTTPAdapter 连接池 32），消除每次请求新建 TCP 连接 |
| 资源采样自适应 | 无 SSE 订阅者时采样降频（2s → 10s），避免空转 `nvidia-smi` |
| 全量扫描 | 目录列表缓存 + 进行中任务路径集合，消除逐文件 `listdir` 与逐文件查库 |
| 前端刷新合并 | 纯进度事件 500ms 合并刷新，状态变化立即刷新，任务列表请求量下降一个数量级 |

### 修复

- 任务完成后 LLM 未卸载导致显存长期占用
- 上下文超限时批次无限重试，改为拆半递归并逐级降粒度
- 译文与原文完全一致（未真正翻译）不再计为有效译文，触发拆批重译并统计比例

### 部署

- Docker 镜像 `linux/amd64`，Python 3.11-slim，内置 ffmpeg 与 CUDA 运行库
- GitHub Actions 自动构建推送 GHCR，标签 `latest` / `1.0.0` / `1.0`
- 非 root 运行（`PUID`/`PGID` 映射），数据与模型目录外挂

> 说明：本项目此前存在内部迭代版本号（1.x → 2.x），开源首发统一收敛为 v1.0.0，功能集以本文件为准。
