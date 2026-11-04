"""ASR 模型下载管理：国内镜像加速 + 真实进度/速度跟踪（v1.6.1 重写）。

faster-whisper 模型经 hf-mirror.com resolve 直链流式下载到
{MODELS_DIR}/{model_name}/ 本地目录（config.json/model.bin/tokenizer.json 等），
用已下载字节数 / Content-Length 计算真实进度与速度，兼容 faster-whisper 本地路径加载。
"""
import logging
import os
import threading
import time

import requests

from . import config

logger = logging.getLogger("subtitle-forge.models")

_lock = threading.Lock()
_downloads = {}  # model_name -> {"status", "size_mb", "percent", "speed_mb_s", "error", "started_at", ...}
_threads = {}

# faster-whisper 仓库中的文件（逐个探测存在性后下载；model.bin 是体积主体）
_REPO_FILES = [
    "config.json",
    "tokenizer.json",
    "vocabulary.txt",
    "normalizer.json",
    "preprocessor_config.json",
    "added_tokens.json",
    "generation_config.json",
    "special_tokens_map.json",
    "model.bin",  # 大文件最后下，保证小文件先就位
]

_UA = "Mozilla/5.0 (X11; Linux x86_64) subtitle-forge/1.6.3"
_TIMEOUT = (10, 30)

# 下载源优先级：国内镜像优先；镜像 404 时回退官方源（可配合 hf_proxy 代理穿透）
_SOURCES = [
    ("hf-mirror", "https://hf-mirror.com/Systran/faster-whisper-{model}/resolve/main"),
    ("hf", "https://huggingface.co/Systran/faster-whisper-{model}/resolve/main"),
]


def _session():
    """带代理的 requests 会话（hf_proxy 设置项，如 http://127.0.0.1:7890）。"""
    proxy = str(config.get_setting("hf_proxy", "") or "").strip()
    proxies = {"http": proxy, "https": proxy} if proxy else None
    s = requests.Session()
    if proxies:
        s.proxies.update(proxies)
    return s


def _model_dir(model_name: str):
    """新布局：模型文件直接平铺目录。"""
    return config.MODELS_DIR / model_name


def _legacy_snapshot_dir(model_name: str):
    """旧布局：huggingface_hub snapshot 缓存目录。"""
    return config.MODELS_DIR / f"models--Systran--faster-whisper-{model_name}"


def _dir_has_file(root, filename):
    if not root or not root.exists():
        return False
    for _root, _dirs, files in os.walk(root):
        if filename in files:
            return True
    return False


def is_model_downloaded(model_name: str) -> bool:
    """模型是否已完整缓存（存在 model.bin 即视为已下载；兼容新旧两种布局）。"""
    return _dir_has_file(_model_dir(model_name), "model.bin") or _dir_has_file(
        _legacy_snapshot_dir(model_name), "model.bin")


def get_models_list():
    """返回可选模型列表及下载状态（含本地已导入模型）。"""
    items = []
    for name, size_mb in config.ASR_MODEL_SIZES_MB.items():
        items.append({
            "name": name,
            "size_mb": size_mb,
            "downloaded": is_model_downloaded(name),
            "downloading": name in _downloads and _downloads[name]["status"] == "downloading",
        })
    # 本地导入模型（MODELS_DIR 下含 model.bin 但不在内置列表中的目录）
    for name in scan_local_models():
        if name not in config.ASR_MODEL_SIZES_MB:
            items.append({
                "name": name,
                "size_mb": _dir_size_mb(_model_dir(name)),
                "downloaded": True,
                "downloading": False,
                "local": True,
            })
    return items


def scan_local_models():
    """扫描 MODELS_DIR 下已含 model.bin 的本地模型目录名。"""
    out = []
    if not config.MODELS_DIR.exists():
        return out
    for d in config.MODELS_DIR.iterdir():
        if d.is_dir() and (d / "model.bin").exists():
            out.append(d.name)
    return sorted(out)


def _dir_size_mb(d):
    total = 0
    for root, _dirs, files in os.walk(d):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return round(total / 1048576, 1)


def import_local_model(src_path: str, model_name: str = ""):
    """从容器内路径导入本地 ASR 模型（复制到 MODELS_DIR/{name}）。返回 (ok, msg, name)。"""
    src = os.path.abspath(os.path.expanduser(src_path or ""))
    if not os.path.isdir(src):
        return False, f"源路径不是目录或不存在: {src}", ""
    if not os.path.exists(os.path.join(src, "model.bin")):
        return False, "目录中未找到 model.bin，无法作为 faster-whisper 模型导入", ""
    name = (model_name or os.path.basename(src.rstrip("/"))).strip()
    if not name:
        return False, "模型名称不能为空", ""
    if any(ch in name for ch in "/\\ \t"):
        return False, "模型名称不能包含路径分隔符或空格", ""
    dest = config.MODELS_DIR / name
    if (dest / "model.bin").exists():
        return False, f"模型 {name} 已存在，无需重复导入", name
    dest.mkdir(parents=True, exist_ok=True)
    try:
        import shutil
        for f in os.listdir(src):
            s = os.path.join(src, f)
            if os.path.isfile(s):
                shutil.copy2(s, dest / f)
        return True, f"本地模型 {name} 导入成功（{_dir_size_mb(dest)} MB）", name
    except Exception as e:
        return False, f"导入失败: {e}", name


def start_download(model_name: str):
    """后台线程下载指定 faster-whisper 模型。返回 (ok, msg)。"""
    if model_name not in config.ASR_MODEL_SIZES_MB:
        return False, f"未知模型: {model_name}"
    if is_model_downloaded(model_name):
        return False, f"{model_name} 已下载，无需重复下载"
    with _lock:
        if _downloads.get(model_name, {}).get("status") == "downloading":
            return False, f"{model_name} 正在下载中"
        _downloads[model_name] = {
            "status": "downloading",
            "size_mb": config.ASR_MODEL_SIZES_MB[model_name],
            "percent": 0.0,
            "speed_mb_s": 0.0,
            "error": None,
            "started_at": time.time(),
            "files": [],
            "total_bytes": 0,
            "done_bytes": 0,
        }
    t = threading.Thread(target=_download_worker, args=(model_name,), daemon=True)
    _threads[model_name] = t
    t.start()
    return True, f"开始下载 {model_name}（经 HF 镜像）"


def _probe_files(model_name: str):
    """多源探测仓库文件（HEAD 跟随重定向拿 Content-Length）。
    返回 (found, source_base)；found 为 [(filename, size)]，source_base 为选中的下载源。
    """
    with _session() as s:
        for source_name, tmpl in _SOURCES:
            base = tmpl.format(model=model_name)
            found = []
            ok = True
            for fn in _REPO_FILES:
                # 429/5xx 限流退避重试（最多 4 次，1s/2s/4s/8s）
                got = False
                for attempt in range(5):
                    try:
                        r = s.head(f"{base}/{fn}", headers={"User-Agent": _UA}, timeout=_TIMEOUT,
                                   allow_redirects=True)
                        if r.status_code in (429, 500, 502, 503, 504) and attempt < 4:
                            r.close()
                            time.sleep(1 << attempt)
                            continue
                        if r.status_code == 200:
                            size = int(r.headers.get("Content-Length") or 0)
                            found.append((fn, size))
                            got = True
                        elif r.status_code == 404:
                            # 源中不存在该文件 → 整个模型不可用，换下一个源
                            ok = False
                        break
                    except Exception as e:
                        if attempt < 4:
                            time.sleep(1 << attempt)
                            continue
                        logger.warning("探测 %s@%s 失败: %s", fn, source_name, e)
                        ok = False
                        break
                if not ok or not got:
                    if ok and not got:
                        ok = False
                    break
            if ok and found:
                # 保证 model.bin 最后下载
                found.sort(key=lambda x: (x[0] != "model.bin",))
                logger.info("模型 %s 使用下载源: %s", model_name, source_name)
                return found, base
    return [], ""


def _download_worker(model_name: str):
    st = _downloads[model_name]
    dest_dir = _model_dir(model_name)
    dest_dir.mkdir(parents=True, exist_ok=True)
    try:
        files, base = _probe_files(model_name)
        if not files:
            raise RuntimeError("仓库文件探测失败：镜像与官方源均不可达或未同步该模型；可在设置里配置下载代理后重试")
        st["files"] = [f[0] for f in files]
        st["total_bytes"] = sum(sz for _, sz in files if sz > 0)
        done = 0.0
        last_update = time.time()
        last_done = 0.0

        def _touch(force=False):
            nonlocal done, last_update, last_done
            now = time.time()
            if not force and now - last_update < 0.5:
                return
            dt = now - last_update
            if dt > 0 and done > last_done:
                st["speed_mb_s"] = round((done - last_done) / dt / 1048576, 2)
            else:
                st["speed_mb_s"] = 0.0
            last_update = now
            last_done = done
            st["done_bytes"] = int(done)
            # 镜像端 HEAD 常拿不到 Content-Length（total_bytes=0），用 size_mb 兜底作分母，
            # 否则会出现"速度在走但百分比恒 0"的假死进度
            total = max(st["total_bytes"], st["size_mb"] * 1048576)
            if total > 0:
                st["percent"] = round(min(99.0, done / total * 100), 1)

        with _session() as s:
            for fn, size in files:
                part = dest_dir / f".{fn}.part"
                # 断点续传：.part 已有大小作为已下载量，用 Range 从断点续传
                part_size = part.stat().st_size if part.exists() else 0
                headers = {"User-Agent": _UA}
                if part_size > 0:
                    headers["Range"] = f"bytes={part_size}-"
                done += part_size
                # 429/5xx 限流退避重试（最多 4 次，1s/2s/4s/8s）
                resp = None
                for attempt in range(5):
                    try:
                        resp = s.get(f"{base}/{fn}", headers=headers, timeout=_TIMEOUT,
                                     stream=True, allow_redirects=True)
                        if resp.status_code in (429, 500, 502, 503, 504) and attempt < 4:
                            resp.close()
                            time.sleep(1 << attempt)
                            continue
                        break
                    except requests.RequestException as e:
                        if attempt < 4:
                            time.sleep(1 << attempt)
                            continue
                        raise RuntimeError(f"{fn} 请求异常: {e}")
                if resp.status_code == 206:
                    mode = "ab"
                elif resp.status_code == 200:
                    # 服务端不支持续传，从头写（回退已计入的 part_size）
                    mode = "wb"
                    done -= part_size
                else:
                    raise RuntimeError(f"{fn} 下载返回 HTTP {resp.status_code}")
                with resp:
                    with open(part, mode) as f:
                        for chunk in resp.iter_content(chunk_size=1 << 16):
                            if not chunk:
                                continue
                            f.write(chunk)
                            done += len(chunk)
                            _touch()
                part.rename(dest_dir / fn)
                _touch(force=True)
        st["status"] = "done"
        st["percent"] = 100.0
        st["speed_mb_s"] = 0.0
        logger.info("模型 %s 下载完成（%.1f MB）", model_name, done / 1048576)
    except Exception as e:
        logger.exception("模型 %s 下载失败", model_name)
        st["status"] = "error"
        st["error"] = str(e)[:300]
        # 保留 .part 半成品：下次点击下载自动断点续传，避免从头重下


def download_status():
    """返回各模型下载进度（含进行中/已完成/失败）。"""
    out = []
    for name, st in list(_downloads.items()):
        item = dict(st)
        item["model"] = name
        item.pop("files", None)
        item.pop("total_bytes", None)
        item.pop("done_bytes", None)
        out.append(item)
    return out


# ---------------------------------------------------------------------------
# v1.8.0: llama-swap 统一托管 ASR（whisper.cpp）
# 一键部署 = 下载 ggml 模型到 llama models 卷 → 更新 llama-swap config.yaml →
#           docker restart llama-swap → 校验转录端点。与 LLM 同一套显存调度。
# ---------------------------------------------------------------------------

# faster-whisper 模型名 → whisper.cpp ggml 文件名（ggerganov/whisper.cpp 仓库）
_GGML_MAP = {
    "tiny": "ggml-tiny.bin", "base": "ggml-base.bin", "small": "ggml-small.bin",
    "medium": "ggml-medium.bin", "large-v3": "ggml-large-v3.bin",
    "large-v3-turbo": "ggml-large-v3-turbo.bin",
    "tiny.en": "ggml-tiny.en.bin", "base.en": "ggml-base.en.bin",
    "small.en": "ggml-small.en.bin", "medium.en": "ggml-medium.en.bin",
}

# whisper.cpp ggml 模型下载源（国内镜像优先，回退官方源）
_GGML_SOURCES = [
    ("hf-mirror", "https://hf-mirror.com/ggerganov/whisper.cpp/resolve/main/{file}"),
    ("hf", "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/{file}"),
]

_deploy_lock = threading.Lock()
_deploys = {}   # model_name -> {"status", "percent", "stage", "error", ...}
_deploy_threads = {}


def llama_models_dir() -> str:
    return str(config.get_setting("llama_models_dir") or "/models-llama")


def llama_config_path() -> str:
    return str(config.get_setting("llama_swap_config") or "/config-llama.yaml")


def llama_swap_container() -> str:
    return str(config.get_setting("llama_swap_container") or "llama-swap")


def is_llama_deployed(model_name: str) -> bool:
    """判断 whisper-{model} 是否已部署到 llama-swap（config 含条目 + ggml 文件存在）。"""
    ggml = _GGML_MAP.get(model_name)
    if not ggml:
        return False
    model_file = os.path.join(llama_models_dir(), "whisper", ggml)
    if not os.path.exists(model_file):
        return False
    cfg = llama_config_path()
    if not os.path.exists(cfg):
        return False
    return f'"whisper-{model_name}"' in open(cfg, encoding="utf-8").read()


def deploy_to_llama(model_name: str):
    """后台线程一键部署 whisper 模型到 llama-swap 托管。返回 (ok, msg)。"""
    if model_name not in _GGML_MAP:
        return False, f"未知模型: {model_name}（llama 托管支持 tiny/base/small/medium/large-v3/large-v3-turbo）"
    with _deploy_lock:
        st = _deploys.get(model_name, {})
        if st.get("status") in ("deploying", "restarting"):
            return False, f"{model_name} 正在部署中"
        _deploys[model_name] = {
            "status": "deploying", "percent": 0.0, "stage": "准备中",
            "provider": "llama", "error": None, "started_at": time.time(),
        }
    t = threading.Thread(target=_deploy_worker, args=(model_name,), daemon=True)
    _deploy_threads[model_name] = t
    t.start()
    return True, f"开始部署 {model_name} 到 llama-swap 托管"


def _deploy_worker(model_name: str):
    st = _deploys[model_name]
    ggml = _GGML_MAP[model_name]
    try:
        # 1) 下载 ggml 模型到 llama models 卷 whisper/ 子目录
        st["stage"] = "下载模型"
        dest_dir = os.path.join(llama_models_dir(), "whisper")
        os.makedirs(dest_dir, exist_ok=True)
        dest = os.path.join(dest_dir, ggml)
        if not (os.path.exists(dest) and os.path.getsize(dest) > 1 << 20):
            _download_ggml_file(model_name, ggml, dest, st)
        else:
            st["percent"] = 60.0
        # 2) 更新 llama-swap config.yaml（追加 whisper-{model} 条目）
        st["stage"] = "写入配置"
        _ensure_whisper_in_config(model_name, ggml)
        st["percent"] = 70.0
        # 3) docker restart llama-swap 加载新配置
        st["stage"] = "重启 llama-swap"
        st["status"] = "restarting"
        _docker_restart(llama_swap_container())
        st["percent"] = 85.0
        # 4) 等待 llama-swap 就绪并校验 whisper 模型可路由
        st["stage"] = "校验服务"
        base = _llama_base()
        ok = False
        for _ in range(60):
            time.sleep(2)
            try:
                if base:
                    r = requests.get(f"{base}/health", timeout=5)
                    if r.status_code == 200:
                        ok = True
                        break
            except Exception:
                pass
        if not ok:
            raise RuntimeError("llama-swap 重启后未就绪（health 检查失败）")
        # 确认模型出现在 OpenAI 模型列表
        if base:
            try:
                r = requests.get(f"{base}/v1/models", timeout=10)
                names = [m.get("id") or m.get("model") for m in r.json().get("data", [])] if r.status_code == 200 else []
                if f"whisper-{model_name}" not in names:
                    logger.warning("llama-swap 模型列表未出现 whisper-%s（当前: %s）", model_name, names[-5:])
            except Exception as e:
                logger.warning("校验 whisper 模型列表失败: %s", e)
        st["status"] = "done"
        st["percent"] = 100.0
        st["stage"] = "部署完成"
        logger.info("whisper-%s 已部署到 llama-swap", model_name)
    except Exception as e:
        logger.exception("部署 whisper-%s 失败", model_name)
        st["status"] = "error"
        st["error"] = str(e)[:300]
        st["stage"] = "失败"


def _download_ggml_file(model_name: str, ggml: str, dest: str, st: dict):
    """下载 whisper.cpp ggml 模型（多源回退，带进度）。"""
    size_mb = config.ASR_MODEL_SIZES_MB.get(model_name, 0) or 500
    part = dest + ".part"
    done = part.stat().st_size if os.path.exists(part) else 0
    last_t = time.time()
    last_done = done
    with _session() as s:
        for src_name, tmpl in _GGML_SOURCES:
            url = tmpl.format(file=ggml)
            headers = {"User-Agent": _UA}
            if done > 0:
                headers["Range"] = f"bytes={done}-"
            try:
                resp = s.get(url, headers=headers, timeout=_TIMEOUT, stream=True, allow_redirects=True)
                if resp.status_code in (429, 500, 502, 503, 504):
                    resp.close()
                    continue
                if resp.status_code not in (200, 206):
                    resp.close()
                    continue
                mode = "ab" if resp.status_code == 206 else "wb"
                if resp.status_code == 200:
                    done = 0
                total = done + int(resp.headers.get("Content-Length") or 0)
                with resp:
                    with open(part, mode) as f:
                        for chunk in resp.iter_content(chunk_size=1 << 16):
                            if not chunk:
                                continue
                            f.write(chunk)
                            done += len(chunk)
                            now = time.time()
                            if now - last_t >= 0.5:
                                base_total = max(total, size_mb * 1048576)
                                st["percent"] = round(min(99.0, done / base_total * 100), 1)
                                last_t = now
                                last_done = done
                os.replace(part, dest)
                return
            except requests.RequestException as e:
                logger.warning("ggml 下载 %s@%s 失败: %s", ggml, src_name, e)
                continue
    raise RuntimeError("ggml 模型下载失败：hf-mirror 与官方源均不可达")


def _ensure_whisper_in_config(model_name: str, ggml: str):
    """把 whisper-{model} 条目写入 llama-swap config.yaml（幂等：已存在则跳过）。

    采用文本插入保留原配置格式；新条目插到 models: 段末尾（下一个顶层键之前）。
    """
    cfg_path = llama_config_path()
    if not os.path.exists(cfg_path):
        raise RuntimeError(f"llama-swap 配置文件不存在: {cfg_path}（请检查挂载）")
    with open(cfg_path, encoding="utf-8") as f:
        content = f.read()
    model_id = f"whisper-{model_name}"
    if f'"{model_id}"' in content or f"'{model_id}'" in content:
        return  # 已部署过
    block = (
        f'  "{model_id}":\n'
        f'    name: "Whisper {model_name} (ASR, llama 托管)"\n'
        f'    cmd: |\n'
        f'      /app/whisper-server\n'
        f'      --model /models/whisper/{ggml}\n'
        f'      --host 127.0.0.1\n'
        f'      --port ${{PORT}}\n'
        f'      --request-path /v1/audio/transcriptions --inference-path ""\n'
    )
    lines = content.splitlines()
    # 找 models: 顶层段，插入到该段末尾（下一个顶层键行之前）
    top_keys = [i for i, ln in enumerate(lines) if ln and not ln.startswith((" ", "\t", "#"))]
    models_idx = next((i for i in top_keys if lines[i].rstrip().rstrip(":").strip() == "models"), None)
    if models_idx is None:
        raise RuntimeError("config.yaml 中未找到 models 顶层键")
    insert_at = len(lines)
    for k in top_keys:
        if k > models_idx:
            insert_at = k
            break
    new_lines = lines[:insert_at] + block.rstrip("\n").splitlines() + lines[insert_at:]
    # 备份后写入
    backup = cfg_path + f".bak-{time.strftime('%Y%m%d%H%M%S')}"
    try:
        import shutil
        shutil.copy2(cfg_path, backup)
    except Exception:
        pass
    with open(cfg_path, "w", encoding="utf-8") as f:
        f.write("\n".join(new_lines) + "\n")
    logger.info("已更新 %s 并备份到 %s", cfg_path, backup)


def _docker_restart(container: str):
    """通过 /var/run/docker.sock 重启指定容器（容器内无 docker CLI，用 curl --unix-socket）。"""
    import subprocess
    sock = "/var/run/docker.sock"
    if not os.path.exists(sock):
        raise RuntimeError("容器内未挂载 /var/run/docker.sock，无法重启 llama-swap")
    r = subprocess.run(
        ["curl", "-s", "-X", "POST", "--unix-socket", sock,
         f"http://localhost/containers/{container}/restart"],
        capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"docker restart {container} 失败: {r.stderr.strip()[:200]}")
    logger.info("已通过 docker.sock 重启 %s", container)


def _llama_base() -> str:
    """容器内访问 llama-swap 的根地址（llm_base_url 去掉 /v1）。"""
    base = str(config.get_setting("llm_base_url") or "").strip().rstrip("/")
    if not base:
        return ""
    return base[:-3].rstrip("/") if base.endswith("/v1") else base


def deploy_status():
    """返回各模型部署进度。"""
    out = []
    for name, st in list(_deploys.items()):
        item = dict(st)
        item["model"] = name
        item.setdefault("provider", "llama")
        out.append(item)
    return out
