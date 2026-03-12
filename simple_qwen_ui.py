# -*- coding: utf-8 -*-
import argparse
from typing import Optional

import gradio as gr
import torch
from qwen_tts import Qwen3TTSModel

from simple_tts_common import transcribe_with_gemini


CHECKPOINT = "Qwen/Qwen3-TTS-12Hz-1.7B-Base"
LANG_CHOICES = ["English", "Vietnamese", "Auto"]

_tts: Optional[Qwen3TTSModel] = None
_load_error: Optional[str] = None


def dtype_from_str(value: str) -> torch.dtype:
    value = (value or "").strip().lower()
    if value in ("bf16", "bfloat16"):
        return torch.bfloat16
    if value in ("fp16", "float16"):
        return torch.float16
    return torch.float32


def ensure_model(device: str, dtype_name: str, flash_attn: bool) -> Qwen3TTSModel:
    global _tts, _load_error
    if _tts is not None:
        return _tts
    if _load_error is not None:
        raise RuntimeError(_load_error)

    try:
        _tts = Qwen3TTSModel.from_pretrained(
            CHECKPOINT,
            device_map=device,
            dtype=dtype_from_str(dtype_name),
            attn_implementation="flash_attention_2" if flash_attn else None,
        )
        return _tts
    except Exception as exc:
        _load_error = str(exc)
        raise


def normalize_language(language: str) -> Optional[str]:
    if not language or language == "Auto":
        return None
    return language


def auto_reference_text(ref_audio: str, language: str):
    if not ref_audio:
        return "", "Chưa có audio mẫu."
    try:
        text = transcribe_with_gemini(ref_audio, language)
        return text, "Đã lấy text từ audio bằng Gemini."
    except Exception as exc:
        return "", str(exc)


def generate(
    ref_audio: str,
    ref_text: str,
    target_text: str,
    language: str,
    xvector_only: bool,
    device: str,
    dtype_name: str,
    flash_attn: bool,
):
    if not ref_audio:
        return None, ref_text, "Chưa có audio mẫu."
    if not target_text or not target_text.strip():
        return None, ref_text, "Chưa có câu cần đọc."

    current_ref_text = (ref_text or "").strip()
    if not xvector_only and not current_ref_text:
        try:
            current_ref_text = transcribe_with_gemini(ref_audio, language)
        except Exception as exc:
            return None, ref_text, f"Không tự lấy được text: {exc}"

    try:
        tts = ensure_model(device, dtype_name, flash_attn)
        wavs, sr = tts.generate_voice_clone(
            text=target_text.strip(),
            language=normalize_language(language),
            ref_audio=ref_audio,
            ref_text=current_ref_text or None,
            x_vector_only_mode=bool(xvector_only),
            non_streaming_mode=False,
        )
        return (sr, wavs[0]), current_ref_text, "Xong."
    except Exception as exc:
        return None, current_ref_text or ref_text, f"Lỗi: {exc}"


def build_demo(device: str, dtype_name: str, flash_attn: bool) -> gr.Blocks:
    with gr.Blocks(title="Qwen Clone") as demo:
        gr.Markdown("# Qwen Clone")
        with gr.Row():
            with gr.Column():
                ref_audio = gr.Audio(label="Audio mẫu", type="filepath")
                ref_text = gr.Textbox(label="Text mẫu", lines=3, placeholder="Để trống rồi bấm 'Lấy text bằng Gemini' hoặc cứ Generate.")
                with gr.Row():
                    auto_btn = gr.Button("Lấy text bằng Gemini")
                    xvector_only = gr.Checkbox(
                        label="Use x-vector only",
                        value=False,
                        info="Nhanh hơn, không cần text mẫu, nhưng độ giống và nhịp nói thường kém hơn.",
                    )
            with gr.Column():
                target_text = gr.Textbox(label="Text cần đọc", lines=5)
                language = gr.Dropdown(LANG_CHOICES, value="English", label="Ngôn ngữ")
                generate_btn = gr.Button("Tạo audio", variant="primary")
                status = gr.Textbox(label="Trạng thái", lines=2)
                audio_out = gr.Audio(label="Kết quả", type="numpy")

        auto_btn.click(
            auto_reference_text,
            inputs=[ref_audio, language],
            outputs=[ref_text, status],
        )
        generate_btn.click(
            lambda ref_audio, ref_text, target_text, language, xvector_only: generate(
                ref_audio,
                ref_text,
                target_text,
                language,
                xvector_only,
                device,
                dtype_name,
                flash_attn,
            ),
            inputs=[ref_audio, ref_text, target_text, language, xvector_only],
            outputs=[audio_out, ref_text, status],
        )

    return demo


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7861)
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--dtype", default="float16")
    parser.add_argument("--flash-attn", action="store_true", default=False)
    args = parser.parse_args()

    demo = build_demo(args.device, args.dtype, args.flash_attn)
    demo.queue(default_concurrency_limit=4).launch(
        server_name=args.host,
        server_port=args.port,
        share=False,
        theme=gr.themes.Soft(),
    )


if __name__ == "__main__":
    main()
