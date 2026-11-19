# Changelog

本项目的版本变更记录。版本号遵循 `MAJOR.MINOR.PATCH`。

## v1.1.17（2026-11-19）

首页统计卡片增强：已完成卡片支持按天查看产出量，两张分布图均可点击交互。

### 新增

- **已完成卡片 · 近 14 天每日完成量**：柱状图展示最近 14 个自然日每天的完成数（末位为今天），`/api/stats` 新增 `done_daily` 字段（`date` / `day` / `count`）。
- 点击某根柱子：卡片说明行变为「MM-DD 完成 N 个」，选中柱高亮、其余柱淡出；再点同一根或点卡片非柱区域即回到总值展示（「近 14 天 完成 N 个」）。
- 今日生成卡片的 24 小时分布同样支持点击卡片上半部分取消选中。

### 调整

- 统计面板卡片顺序：**已完成** 与 **累计生成** 互换位置，已完成排在首位，便于承载柱状图；今日生成保持第二位。

### 修复

- 首页统计每 10 秒轮询刷新时会把已选中的时段/日期重置，导致刚点开的数值一闪就消失；现改为刷新时保留选中态。
- **修复柱状图点不中/点了没反应**：柱子改为整格全高可点单元（移动端加高至 36px），并按横向坐标映射到时段/日期，无需精准点中细柱；同时修掉「选中后重绘导致冒泡目标断连、被取消监听立刻清空」的根因。
- 新增手势：点击卡片以外的任意区域也会取消选中、回到总值展示；点击卡片上半部分同样取消。

## v1.1.15（2026-11-19）

新增「快翻模式」与中间环节独立开关：除「提取音频 → 转录 → 翻译」外，其余环节均可关闭。

### 新增

- `fast_mode` 快翻总开关（设置页「快翻模式」）：开启后强制关闭转录质检、翻译后校对、大模型润色，翻译完成即落盘交付。
- `transcript_check_enabled` 转录质检开关（默认开启）：关闭后转录完成直接进入翻译，不再做规则校验；一键「只转录」也同步生效。
- `review_enabled` 翻译后校对开关在设置页可见并持久化。
- 新增**转录废料强制拦截** `quality_check.quick_guard`：空白、整段同一句、重复泛滥、占位废文本、覆盖率近似 0、有效字符与时长严重不匹配 → 任务直接判失败并归档，不会进入翻译；该拦截不受「转录质检」开关与快翻模式影响。

### 说明

- 「仅翻译已有字幕」（`translate_only`）可连同提取音频与转录一起跳过，只翻译已有字幕，是最省时的组合。
- 关闭质检/校对仅影响是否把关、是否调用大模型，不影响字幕落盘路径与命名。

## v1.1.14（修复）

任务列表状态标识中英混排：`translated` / `reviewed` 两个 sync 中间态在 `STAGE_NAMES` 里没有中文文案，前端回退显示原始状态名。

### 修复

- `STAGE_NAMES` 补 `translated:'待校对'`、`reviewed:'待润色'`，状态标识全中文。
- 任务操作按钮的「取消」由状态白名单驱动，白名单同步加入 `translated` / `reviewed`，中间态任务可正常取消。
- 「等候中」悬停提示改为四段口径：排队中 + 待翻译 + 待校对 + 待润色（与 v1.1.13 的 `waiting` 聚合口径一致）。

## v1.1.13（2026-11-16）

首页统计 500 修复：`/api/stats` 抛 `IndexError: No item with that key`，导致首页所有统计卡显示「–」、任务页计数全 0。

### 修复

- `database.task_stats()`：聚合 SQL 补上 `waiting_translated` / `waiting_reviewed` 两个字段（v1.1.11 引入 sync 中间态时只加了 keys 没加 SQL），并把「等候中」口径修正为 `pending + transcribed + translated + reviewed`。
- `database.task_stats()`：结果取值改为缺失字段按 0 兜底（`k in r.keys()`），避免 SQL 与 keys 再次不同步时整站统计直接 500。

### 实测

- 修复前：`GET /api/stats` → 500 `IndexError: No item with that key`（database.py:394），首页统计卡全「–」。
- 修复后：`/api/stats` 200，total/waiting/done/failed 等字段齐全，首页卡片与任务页计数恢复。

## v1.1.12（2026-11-16）

批次完整性修复：避免看门狗误判把批次切碎成 1~2 个成员，保证 20 个任务真正整批同步推进。

### 修复

- **看门狗误判长批次**：`watchdog_stuck_seconds` 默认值 60s → 300s（下限 15s → 120s），`watchdog_idle_seconds` 默认 180s → 300s（下限 60s → 120s）。翻译/校对/润色是「整批提交、整批等待」，单批 200 条最长需 1~2 分钟，原阈值会把正常批次判成卡死并触发 repair。
- **repair 切碎批次**：`repair()` 解除批次死锁时改为保留本批所有活动成员（含 `transcribed/translated/reviewed` 中间态），不再缩成「正在跑的 1~2 个」。
- **批次不会补满**：`_admit_batch()` 在批次处于转录阶段（成员全为待转录/转录中）时按 `batch_size` 补满名额；一旦有成员进入阶段中间态或翻译/校对/润色中，批次立即锁死不再补人，保证整批阶段同步、杜绝跨阶段混跑。

### 实测

- 修复前：看门狗在 14:16 因「任务无进度超过 60s」触发 repair，批次从 20 缩到 2，日志出现 `批次状态 {'translated': 2}`，20 阶段同步退化为 2 个任务同步。
- 修复后：批次维持 20 个成员，阶段闸门日志显示整批推进（`translate → review`、`review → polish`），无跨阶段混跑。

## v1.1.11（2026-11-16）

阶段同步调度版：新增 `sync` 调度模式，整批任务按「转录 → 翻译 → 校对 → 润色」四阶段集体推进，
杜绝「单个任务把大模型来回切换、跑完全流程才排下一个」。

### 新增：sync 批内阶段同步调度

- `queue_manager.py` 新增 `_sync_stage_of()` / `_sync_checkpoint()`：按批次状态分布推断当前阶段，上一环节还有任务在跑就锁死本阶段，整批到边界才一次性提交下一阶段。
- 阶段推进链：`pending/transcribing` → 转录；整批 `transcribed` → 翻译；整批 `translated` → 校对；整批 `reviewed` → 润色。
- 新增 `_review_stage()`（翻译质检，落 `translated`）、`_polish_stage()`（润色，落 `done`）、`_load_translated_srt()`（从磁盘还原译文）。
- 新增 `translated` / `reviewed` 阶段中间态，并纳入 `BATCH_ACTIVE_STATUSES`（仍占批次名额）。
- 阶段切换由 `_sync_prepare()` 只预热一次目标模型；转录阶段自动卸载 LLM 让位给 ASR。
- 混跑防护：`_sync_stage_of()` 12 例纯逻辑单测全通过，确保不会出现「有的任务已校对、有的还在翻译」。

### 配套改动

- `database.py`：`waiting` 口径扩展为 `pending + transcribed + translated + reviewed`，新增 `waiting_translated` / `waiting_reviewed` 自检字段；任务视图 `all` / `cur` / `check` / `translate` / `polish` 纳入两个新中间态。
- `main.py`：启动恢复时 sync 中间态原样入队续跑；暂停放开到中间态，恢复时回到暂停前阶段（`meta.paused_from`），避免被重置为 `pending` 重跑转录。
- 前端：新增「sync」调度模式选项；状态名与配色补充「待校对」「待润色」；首页「等待队列」悬停明细含四阶段待跑数。
- 默认行为：`scheduler_mode=auto` 在显存不足以多模型共存时解析为 `sync`（RTX 5060 8G 属单模型场景）。

## v1.1.10（2026-11-16）

统计自洽版：修复首页队列统计「各项之和对不上总任务数」的口径缺陷。

### 修复：等候中与总任务数对不上

- 根因一：「等候中」取自调度器内存队列 `batchState.waiting`（只含本次运行已入队的任务），而「总任务数 / 已完成 / 失败 / 进行中」取自数据库全量统计，两者口径不同。
- 根因二：`transcribed`（已转录、待翻译）这一中间态没被任何卡片统计，成为「消失的任务」；`stats.waiting` 原来只算 `pending`。
- 实测差额：2942 −（1 + 1375 + 1473 + 47）= 46，其中 27 条为 `transcribed`，19 条为库内 `pending` 但未进入调度队列。
- 修复：`database.py: task_stats()` 的 `waiting` 改为 `pending + transcribed`，并新增 `waiting_pending` / `waiting_transcribed` / `other`（自检差额）字段。
- 前端：「等候中」改用库内口径 `s.waiting`，调度队列数降级为鼠标悬停提示；「总任务数」悬停显示 `等候 + 进行中 + 已完成 + 失败 = 总数` 的自洽算式。
- 首页「等待队列」原为 `waiting + active` 重复计数（`active` 已含 `pending`），改为只取 `waiting`，副标题改为「排队中 + 待翻译」。

## v1.1.9（2026-11-16）

统计口径修正版：「累计生成」不再把已取消任务计入；「不支持」从恒为 0 的死指标改为真实可产出的指标。

### 修复：累计生成排除已取消任务

- `database.py: task_stats()` 的 `total` 由 `COUNT(*)` 改为 `SUM(CASE WHEN status!='cancelled' THEN 1 ELSE 0 END)`。
- 实测 7371（含 4429 条 cancelled）→ 2942，只统计实际进入处理流程的任务。
- 首页卡片副标题由「全部任务数」改为「不含已取消」，避免口径误读。

### 修复：不支持统计恒为 0（死指标）

- 根因：原口径为 `status='failed' AND error LIKE '%不支持%'`，但全项目没有任何一处写入「不支持」字样，
  格式不支持的视频在扫描阶段就按 `video_exts` 白名单过滤掉、从不建任务，因此该值永远为 0 —— 不是故障，但属于无效指标。
- `watcher.py` 新增 `OTHER_VIDEO_EXTS`（`.rmvb/.rm/.vob/.m4v/.3gp/.asf/.mpg/.mpeg/.f4v/.ogv/...` 等常见但不在白名单的容器），
  全量扫描与「已有字幕」轻量重算时逐文件计数，计入 `LAST_SCAN_SKIP["unsupported"]`；`classify_video` 对这类文件返回
  「不支持的视频格式」而非「非视频文件」。
- `main.py` 在扫描结束与统计刷新时把该计数落盘为 `scan_unsupported_count`。
- `database.py: task_stats()` 的 `unsupported` 改为两部分取大：任务级 `failed` 且错误属格式/编码不支持
  （`不支持 / 无法解析 / Invalid data / 提取音频失败 / no audio / 没有音频`）+ 扫描级 `scan_unsupported_count`；
  同时新增 `task_unsupported` / `scan_unsupported` 两个分项字段便于排查。
- 实测：`unsupported=2`（MIGD-510.mp4、STAR-456-C.mp4，AAC 音轨损坏导致 ffmpeg 解码失败）；
  当前媒体库 2858 个视频中非白名单容器 0 个，故 `scan_unsupported=0`。
- 首页副标题改为「格式/编码不支持」。

### 其它

- `APP_VERSION` / `VERSION` / 运行目录 `docker-compose.yml` 镜像 tag → `1.1.9`。

## v1.1.8（2026-11-16，补记）

- 「今日生成」改按完成时间（`updated_at`）统计本地当天 00:00–24:00，新增 24 小时时段分布柱状条。
- 新增「已有字幕」统计：扫描时单独计数 `scan_existing_count` 落盘，并支持手动 / 启动后超 12 小时自动重算
  （`POST /api/stats/existing/refresh`）。

## v1.1.7（2026-11-15）

功能版本：新增「最长视频时长」过滤（默认 240 分钟），并让「恢复队列」默认接入任务修复。

### 新增：最长视频时长过滤

- 新增设置项 `max_video_minutes`（`MAX_VIDEO_MINUTES`，默认 `240`，区间 `0~2880`，`0` = 不限制）：
  超过上限的视频不再进入任务队列，避免整部超长素材（演唱会 / 直播回放 / 课程合集）占用 GPU 数小时。
- `watcher.py` 新增 `max_video_minutes()` / `duration_seconds()` / `duration_exceeded()`：
  ffprobe 探测时长，探测失败（返回 0）时放行，不因探测异常把正常视频挡在队列外。
- 全量扫描 `scan_dirs`：逐文件判定，超长计入 `skipped_duration`，扫描进度面板显示「超长排除 >N 分钟」。
- 一键转录 / 一键翻译：同样在入队前判定，超长写入日志并计入跳过数，任务 meta 记录 `duration`。
- 一键润色：由字幕反查同目录原视频（`_video_of_subtitle`）后判定，无法反查时放行。
- 手动添加（`/api/tasks`）：单文件超长直接返回明确错误；目录添加返回 `skipped_duration` 与 `limit_minutes`。
- 设置页新增「最长视频时长」输入框（默认 240），保存时夹取 `0~2880`。

### 变更：恢复队列默认接入任务修复

- `resume_all(force_unload=True)`：恢复队列默认走一次 `repair`（卸载无任务的闲置大模型 → 强杀卡住的提取进程 →
  解除批次闸门死锁 → 中间态归位 → 重建工作线程 → 重新入队并预热工作模型），而不是单纯把 `paused` 改回 `pending`。
- `POST /api/queue/pause` 的 `force_unload` 默认改为 `true`，响应带回 `repaired` 与最近一次修复快照；
  只想纯放行时显式传 `force_unload=false`。
- 前端「恢复队列」按钮与独立 `resumeQueue()` 统一走修复路径，提示文案展示卸载/终止等修复动作详情。
- `APP_VERSION` / `VERSION` / 镜像 tag 同步为 `1.1.7`。

## v1.1.6（2026-11-14）

修复：扫描任务无法取消 —— 误点「扫描全部 / 一键转录 / 一键翻译 / 一键润色」后只能等它扫完，
取消监控目录勾选也不起作用，且清空任务后又被扫描器重新塞满。

### 问题

- 四个扫描入口都是同步遍历，前端只 toast 一句，没有任何进度和取消入口；页面刷新后彻底失联。
- 扫描启动时固定了目录快照，扫描过程中移除监控目录（取消勾选）不会影响已在跑的遍历。
- `scan_polish_jobs` / `scan_transcribe_jobs` / `scan_translate_jobs` 用 `sorted(rglob(...))`
  先全量枚举再排序，大目录既慢又让取消点失效。
- 「清空记录」只清数据库，扫描线程仍在往队列塞任务，清完立刻又满。

### 修复

- 新增 `app/scan_control.py`：进程级 `ScanController`，登记扫描任务、上报进度、支持取消；
  同一时间只允许一个扫描在跑，避免重复遍历同一批目录。
- `watcher.scan_dirs` 与 `queue_manager.scan_*_jobs` 全部支持 `cancel_cb` + `on_progress`：
  每 200 / 100 个文件与每个目录轮询一次取消标记，触发即停止并返回已入队数量。
- 去掉 `sorted(rglob(...))`，改为惰性遍历，保证大目录下取消能及时生效。
- 扫描过程中每个顶层目录开始前重新读取监控目录列表，已被移除的目录立即跳过。
- 四个扫描接口改为后台线程执行，立即返回 `job_id`；新增：
  - `GET /api/scan/status` 运行中进度 + 最近一次结果；
  - `POST /api/scan/cancel` 取消扫描，`purge` 可连带清理本次扫描入队且尚未开始的任务。
- `SmartQueue.clear_pending(sources, delete)`：按来源清理未开始的任务（默认标记 cancelled，可选用删除）。
- `/api/tasks/cancel-all`、`/api/tasks/clear-all` 默认先取消扫描再清任务，避免清完又被塞满。
- 前端新增扫描面板：实时进度（已检查/入队/跳过/当前路径/耗时）+「取消扫描」「取消并清空排队」按钮，
  通过 SSE `scan_update` 与 1.5s 轮询同步；扫描结束后保留结果条可手动关闭。

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
