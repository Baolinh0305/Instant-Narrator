# -*- coding: utf-8 -*-
import argparse
import json
import os
import threading
import traceback
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

for _legacy_cache_env in (
    "TRANSFORMERS_CACHE",
    "PYTORCH_TRANSFORMERS_CACHE",
    "PYTORCH_PRETRAINED_BERT_CACHE",
):
    os.environ.pop(_legacy_cache_env, None)

import numpy as np
import torch
from huggingface_hub import snapshot_download
from qwen_tts import Qwen3TTSModel

from simple_tts_common import transcribe_with_gemini


CHECKPOINT = "Qwen/Qwen3-TTS-12Hz-1.7B-Base"
APP_ROOT = Path(__file__).resolve().parent
HF_HOME = Path(os.environ.get("HF_HOME") or (APP_ROOT / "models-cache" / "huggingface"))
HF_HUB_CACHE = Path(os.environ.get("HF_HUB_CACHE") or (HF_HOME / "hub"))
_tts: Optional[Qwen3TTSModel] = None
_load_error: Optional[str] = None
_device = "cuda:0"
_dtype_name = "float16"
_flash_attn = False
_infer_lock = threading.Lock()
_model_init_lock = threading.Lock()
_warmup_started = False
LOG_PATH = Path(__file__).resolve().parent / "app-data" / "qwen-service.log"


def dtype_from_str(value: str) -> torch.dtype:
    value = (value or "").strip().lower()
    if value in ("bf16", "bfloat16"):
        return torch.bfloat16
    if value in ("fp16", "float16"):
        return torch.float16
    return torch.float32


def resolve_checkpoint_path() -> str:
    direct = Path(CHECKPOINT)
    if direct.exists():
        return str(direct.resolve())
    snapshot_root = HF_HUB_CACHE / "models--Qwen--Qwen3-TTS-12Hz-1.7B-Base" / "snapshots"
    if snapshot_root.exists():
        candidates = sorted(
            [path for path in snapshot_root.iterdir() if path.is_dir() and (path / "config.json").exists()],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return str(candidates[0].resolve())
    HF_HUB_CACHE.mkdir(parents=True, exist_ok=True)
    return snapshot_download(
        repo_id=CHECKPOINT,
        cache_dir=str(HF_HUB_CACHE),
        local_files_only=False,
    )


def ensure_model() -> Qwen3TTSModel:
    global _tts, _load_error
    if _tts is not None:
        return _tts
    if _load_error is not None:
        raise RuntimeError(_load_error)
    with _model_init_lock:
        if _tts is not None:
            return _tts
        if _load_error is not None:
            raise RuntimeError(_load_error)
        try:
            checkpoint_path = resolve_checkpoint_path()
            _tts = Qwen3TTSModel.from_pretrained(
                checkpoint_path,
                device_map=_device,
                dtype=dtype_from_str(_dtype_name),
                attn_implementation="flash_attention_2" if _flash_attn else None,
            )
            return _tts
        except Exception as exc:
            _load_error = str(exc)
            raise


def start_background_warmup() -> None:
    global _warmup_started
    with _model_init_lock:
        if _warmup_started:
            return
        _warmup_started = True

    def _runner() -> None:
        print("Loading Qwen model...", flush=True)
        try:
            ensure_model()
            print("Qwen model ready.", flush=True)
        except Exception as exc:
            log_service_error(exc, {"stage": "warmup"})
            print(f"Qwen model preload failed: {exc}", flush=True)

    threading.Thread(target=_runner, daemon=True).start()


def normalize_language(language: str) -> Optional[str]:
    if not language or language == "Auto":
        return None
    return language


def save_wave(output_path: str, sample_rate: int, wav: np.ndarray) -> None:
    arr = np.asarray(wav, dtype=np.float32)
    arr = np.clip(arr, -1.0, 1.0)
    pcm = (arr * 32767.0).astype(np.int16)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(int(sample_rate))
        handle.writeframes(pcm.tobytes())


def log_service_error(exc: Exception, payload: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write("\n=== Qwen Service Error ===\n")
        handle.write(f"payload={json.dumps(payload, ensure_ascii=False)}\n")
        handle.write("traceback:\n")
        handle.write(traceback.format_exc())
        handle.write("\n")


def generate(payload: dict) -> dict:
    ref_audio = str(payload.get("ref_audio") or "").strip()
    ref_text = str(payload.get("ref_text") or "").strip()
    target_text = str(payload.get("target_text") or "").strip()
    language = str(payload.get("language") or "English").strip() or "English"
    xvector_only = bool(payload.get("xvector_only"))
    output_path = str(payload.get("output_path") or "").strip()

    if not ref_audio:
        raise RuntimeError("Missing ref_audio.")
    if not target_text:
        raise RuntimeError("Missing target_text.")
    if not output_path:
        raise RuntimeError("Missing output_path.")

    current_ref_text = ref_text
    if not xvector_only and not current_ref_text:
        current_ref_text = transcribe_with_gemini(ref_audio, language)

    with _infer_lock:
        tts = ensure_model()
        wavs, sr = tts.generate_voice_clone(
            text=target_text,
            language=normalize_language(language),
            ref_audio=ref_audio,
            ref_text=current_ref_text or None,
            x_vector_only_mode=xvector_only,
            non_streaming_mode=False,
        )
        save_wave(output_path, sr, wavs[0])
    return {
        "ok": True,
        "output_path": output_path,
        "ref_text": current_ref_text,
        "sample_rate": int(sr),
    }


def transcribe(payload: dict) -> dict:
    ref_audio = str(payload.get("ref_audio") or "").strip()
    language = str(payload.get("language") or "English").strip() or "English"
    if not ref_audio:
        raise RuntimeError("Missing ref_audio.")
    return {"ok": True, "ref_text": transcribe_with_gemini(ref_audio, language)}


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: dict) -> bool:
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return True
        except OSError:
            return False

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"ok": True, "model_ready": _tts is not None, "load_error": _load_error})
            return
        self._send(404, {"ok": False, "error": "Not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
            if self.path == "/generate":
                body = generate(payload)
            elif self.path == "/transcribe":
                body = transcribe(payload)
            else:
                self._send(404, {"ok": False, "error": "Not found"})
                return
            if not self._send(200, body):
                return
        except Exception as exc:
            log_service_error(exc, payload if "payload" in locals() else {})
            self._send(200, {"ok": False, "error": str(exc)})

    def log_message(self, format, *args):
        return


def main():
    global _device, _dtype_name, _flash_attn
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7862)
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--dtype", default="float16")
    parser.add_argument("--flash-attn", action="store_true", default=False)
    args = parser.parse_args()

    _device = args.device
    _dtype_name = args.dtype
    _flash_attn = args.flash_attn

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    print(f"Qwen service listening on http://{args.host}:{args.port}", flush=True)
    start_background_warmup()
    server.serve_forever()


if __name__ == "__main__":
    main()
