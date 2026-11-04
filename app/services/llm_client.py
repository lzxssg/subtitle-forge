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
        max_attempts = 6
        last_err = None
        for attempt in range(max_attempts):
            try:
                resp = _session.post(url, headers=headers, json=payload, timeout=300)
            except (requests.ConnectionError, requests.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                last_err = e
                logger.warning("LLM 请求连接异常（第%d/%d次）: %s",
                               attempt + 1, max_attempts, e)
                if attempt < max_attempts - 1:
                    time.sleep(3 + attempt * 4)
                    continue
                raise RuntimeError(f"LLM 请求连接失败: {last_err}")
            if resp.status_code == 200:
                break
            last_err = resp
            if resp.status_code in (429, 500, 502, 503, 504):
                logger.warning("LLM API %s（第%d/%d次），等待模型就绪后重试",
                               resp.status_code, attempt + 1, max_attempts)
                if attempt < max_attempts - 1:
                    time.sleep(3 + attempt * 4)
                    continue
            raise RuntimeError(f"LLM API 错误 {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {}) or {}
        return {
            "content": content,
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
        }


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
