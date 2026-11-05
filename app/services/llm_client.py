"""OpenAI 兼容 LLM 客户端：支持 Ollama / DeepSeek / OpenAI / Gemini 等任意兼容接口。"""
import logging
import time

import requests

from .. import config

logger = logging.getLogger("subtitle-forge.llm")

_client = None
_review_client = None
_polish_client = None

# 复用连接池：翻译/质检为高频短请求，避免每次新建 TCP/TLS 连接
_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=32, max_retries=0)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)


class LLMClient:
    def __init__(self, review=False, polish=False):
        self.base_url = ""
        self.api_key = ""
        self.model = ""
        self.temperature = 0.3
        self.ready = False
        self.is_review = review
        self.is_polish = polish
        self.system_prompt = (
            "你是一个专业的影视字幕翻译助手。只输出要求的格式，不要任何额外说明。"
        )
        self.reload()

    def reload(self):
        if self.is_polish:
            # 润色模型：独立配置，未配置时依次回退 review_* → llm_*
            self.base_url = str(config.get_setting("polish_base_url") or "").strip().rstrip("/")
            self.api_key = str(config.get_setting("polish_api_key") or "").strip()
            self.model = str(config.get_setting("polish_model") or "").strip()
            if not self.model:
                self.model = str(config.get_setting("review_model") or "").strip()
            if not self.base_url:
                self.base_url = str(config.get_setting("review_base_url") or "").strip().rstrip("/")
            if not self.api_key:
                self.api_key = str(config.get_setting("review_api_key") or "").strip()
            if not self.model:
                self.model = str(config.get_setting("llm_model") or "").strip()
            if not self.base_url:
                self.base_url = str(config.get_setting("llm_base_url") or "").strip().rstrip("/")
            if not self.api_key:
                self.api_key = str(config.get_setting("llm_api_key") or "").strip()
            self.temperature = float(config.get_setting("polish_temperature") or 0.3)
        elif self.is_review:
            self.base_url = str(config.get_setting("review_base_url") or "").strip().rstrip("/")
            self.api_key = str(config.get_setting("review_api_key") or "").strip()
            self.model = str(config.get_setting("review_model") or "").strip()
            # 校验模型未独立配置时回退用翻译模型
            if not self.model:
                self.model = str(config.get_setting("llm_model") or "").strip()
            if not self.base_url:
                self.base_url = str(config.get_setting("llm_base_url") or "").strip().rstrip("/")
            self.temperature = 0.2
        else:
            self.base_url = str(config.get_setting("llm_base_url") or "").strip().rstrip("/")
            self.api_key = str(config.get_setting("llm_api_key") or "").strip()
            self.model = str(config.get_setting("llm_model") or "").strip()
            self.temperature = float(config.get_setting("llm_temperature") or 0.3)
        self.ready = bool(self.base_url and self.model)
        if self.base_url and not self.base_url.endswith("/v1") and "/v1" not in self.base_url:
            # 兼容 Ollama http://host:11434 不带 /v1 的写法
            self.base_url = self.base_url + "/v1"

    def chat(self, prompt: str, json_mode: bool = False, max_tokens: int = 4096) -> dict:
        if not self.ready:
            raise RuntimeError("LLM 未配置")
        url = f"{self.base_url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key and self.api_key.lower() not in ("", "ollama", "none"):
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": prompt},
            ],
            "temperature": self.temperature,
            "max_tokens": max_tokens,
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}
        # Sakura 翻译模型复读抑制：低温度 + repetition penalty，防止长文本复读幻觉
        if not self.is_review and "sakura" in (self.model or "").lower():
            payload["temperature"] = 0.1
            payload["repeat_penalty"] = 1.1
        # 校验模型若为 Qwen3/GLM 系（默认带思考流程），显式关闭 thinking 以提速、避免 token 被思考吃掉
        if (self.is_review or self.is_polish) and _need_disable_thinking(self.model):
            payload["chat_template_kwargs"] = {"enable_thinking": False}
        # v2.1.0: 502/连接异常兜底重试。低显存单模型切换时上游进程会重启，
        # 在途请求偶发 EOF/502，重试等待模型就绪后即可成功。
        # v2.3.0: 增加总时间预算，避免上游持续崩溃时单次调用把工作线程占住半小时以上。
        def _int_setting(key, default, lo, hi):
            try:
                return max(lo, min(hi, int(float(config.get_setting(key) or default))))
            except (TypeError, ValueError):
                return default

        max_attempts = _int_setting("llm_max_attempts", 3, 1, 10)
        attempt_timeout = _int_setting("llm_timeout", 180, 20, 1800)
        total_budget = _int_setting("llm_total_budget", 360, 30, 7200)
        deadline = time.time() + total_budget
        last_err = None
        resp = None
        recovered = False
        for attempt in range(max_attempts):
            remain = deadline - time.time()
            if remain <= 5:
                break
            try:
                resp = _session.post(url, headers=headers, json=payload,
                                     timeout=(10, min(attempt_timeout, remain)))
            except (requests.ConnectionError, requests.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                last_err = e
                logger.warning("LLM 请求连接异常（第%d/%d次）: %s",
                               attempt + 1, max_attempts, e)
                # llama-swap 上游进程崩溃后可能进入假死：卸载模型强制其重建，避免整条流水线干等
                if not recovered and isinstance(e, requests.Timeout):
                    recovered = True
                    self._recover_model(e)
                if attempt < max_attempts - 1:
                    pause = min(3 + attempt * 4, max(1, deadline - time.time() - 2))
                    time.sleep(pause)
                    continue
                raise RuntimeError(f"LLM 请求连接失败: {last_err}")
            if resp.status_code == 200:
                break
            last_err = resp
            if resp.status_code in (429, 500, 502, 503, 504):
                logger.warning("LLM API %s（第%d/%d次），等待模型就绪后重试",
                               resp.status_code, attempt + 1, max_attempts)
                if attempt < max_attempts - 1:
                    pause = min(3 + attempt * 4, max(1, deadline - time.time() - 2))
                    time.sleep(pause)
                    continue
            raise RuntimeError(f"LLM API 错误 {resp.status_code}: {resp.text[:300]}")
        else:
            resp = None
        if resp is None or resp.status_code != 200:
            raise RuntimeError(f"LLM 调用超时或失败（预算 {total_budget}s）: {last_err}")
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {}) or {}
        return {
            "content": content,
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
        }

    def _recover_model(self, err) -> None:
        """请求超时（多为上游进程崩溃后假死）时尝试卸载模型，让 llama-swap 重新拉起。"""
        try:
            from . import llm_manager
        except Exception:
            return
        try:
            ok = llm_manager.unload_model(self.model)
            logger.warning("LLM 请求超时（%s），已尝试卸载模型 %s 以触发重建：%s",
                           type(err).__name__, self.model, ok)
        except Exception as e:  # noqa: BLE001
            logger.warning("LLM 模型 %s 卸载失败（忽略）: %s", self.model, e)

    def warmup(self, timeout: float = 90.0) -> bool:
        """阶段开始前把模型拉起来（llama-swap 懒加载），避免首个业务批次撞 502。

        发一个 max_tokens=1 的极小请求，遇到 502/503/连接异常说明模型正在加载，
        轮询等待直到就绪或超时。失败只告警，不阻断流程（业务请求仍有自身重试）。
        """
        if not self.ready:
            return False
        url = f"{self.base_url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key and self.api_key.lower() not in ("", "ollama", "none"):
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": "hi"}],
            "temperature": 0,
            "max_tokens": 1,
            "stream": False,
        }
        if _need_disable_thinking(self.model):
            payload["chat_template_kwargs"] = {"enable_thinking": False}
        t0 = time.time()
        deadline = t0 + timeout
        probe = 0
        while time.time() < deadline:
            probe += 1
            try:
                resp = _session.post(url, headers=headers, json=payload, timeout=30)
                if resp.status_code == 200:
                    logger.info("LLM 模型 %s 预热完成（%.1fs / %d 次探测）",
                                self.model, time.time() - t0, probe)
                    return True
                if resp.status_code not in (429, 500, 502, 503, 504):
                    logger.warning("LLM 模型 %s 预热返回 %s，跳过预热",
                                   self.model, resp.status_code)
                    return False
                logger.info("LLM 模型 %s 加载中（HTTP %s），稍后重探",
                            self.model, resp.status_code)
            except (requests.ConnectionError, requests.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                logger.info("LLM 预热连接异常：%s", str(e)[:80])
            time.sleep(4)
        logger.warning("LLM 模型 %s 预热超时（%.0fs），业务请求将自行重试",
                       self.model, timeout)
        return False


def get_client() -> LLMClient:
    global _client
    if _client is None:
        _client = LLMClient(review=False)
    else:
        _client.reload()
    return _client


def get_review_client() -> LLMClient:
    """独立校验模型客户端：未单独配置时自动回退用翻译模型。"""
    global _review_client
    if _review_client is None:
        _review_client = LLMClient(review=True)
    else:
        _review_client.reload()
    return _review_client


def get_polish_client() -> LLMClient:
    """独立润色模型客户端：未配置时依次回退 review_model → llm_model。"""
    global _polish_client
    if _polish_client is None:
        _polish_client = LLMClient(polish=True)
    else:
        _polish_client.reload()
    return _polish_client


def _need_disable_thinking(model: str) -> bool:
    """Qwen3/GLM 系模型默认带 thinking 流程，质检场景需关闭以提速。"""
    m = (model or "").lower()
    return any(k in m for k in ("qwen", "35b", "glm", "review", "think", "3b"))
