# -*- coding: utf-8 -*-
import base64
import json
import mimetypes
import os
import urllib.error
import urllib.request
from pathlib import Path


GEMINI_MODEL = "gemini-2.5-flash"
MAX_INLINE_AUDIO_BYTES = 18 * 1024 * 1024


def get_api_key() -> str:
    return os.environ.get("GEMINI_API_KEY", "").strip()


def detect_mime_type(audio_path: str) -> str:
    guessed, _ = mimetypes.guess_type(audio_path)
    if guessed:
        return guessed

    ext = Path(audio_path).suffix.lower()
    mapping = {
        ".wav": "audio/wav",
        ".mp3": "audio/mpeg",
        ".m4a": "audio/mp4",
        ".aac": "audio/aac",
        ".flac": "audio/flac",
        ".ogg": "audio/ogg",
        ".webm": "audio/webm",
    }
    return mapping.get(ext, "application/octet-stream")


def transcribe_with_gemini(audio_path: str, language: str = "Auto") -> str:
    audio_path = str(audio_path)
    if not audio_path or not os.path.exists(audio_path):
        raise RuntimeError("Không thấy file audio mẫu.")

    api_key = get_api_key()
    if not api_key:
        raise RuntimeError("Thiếu GEMINI_API_KEY để tách chữ từ audio.")

    size = os.path.getsize(audio_path)
    if size > MAX_INLINE_AUDIO_BYTES:
        raise RuntimeError("Audio mẫu quá lớn cho Gemini inline. Hãy dùng clip ngắn hơn.")

    with open(audio_path, "rb") as f:
        audio_b64 = base64.b64encode(f.read()).decode("ascii")

    language_hint = ""
    if language and language != "Auto":
        language_hint = f"The spoken language is {language}. "

    prompt = (
        f"{language_hint}"
        "Transcribe this audio exactly. Return only the transcript text. "
        "No quotes, no labels, no explanation."
    )

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt},
                    {
                        "inline_data": {
                            "mime_type": detect_mime_type(audio_path),
                            "data": audio_b64,
                        }
                    },
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0
        },
    }

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={api_key}"
    )
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Gemini tách chữ thất bại: {body}") from exc
    except Exception as exc:
        raise RuntimeError(f"Không gọi được Gemini để tách chữ: {exc}") from exc

    candidates = data.get("candidates", [])
    for candidate in candidates:
        content = candidate.get("content", {})
        parts = content.get("parts", [])
        texts = [part.get("text", "").strip() for part in parts if part.get("text")]
        merged = " ".join(texts).strip()
        if merged:
            return merged

    raise RuntimeError("Gemini không trả transcript hợp lệ.")
