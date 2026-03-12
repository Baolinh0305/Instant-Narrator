# -*- coding: utf-8 -*-
import argparse
from typing import Optional

import gradio as gr

from simple_tts_common import transcribe_with_gemini


_infer = None
_default_model = None
_load_error = None


def ensure_f5_loaded():
    global _infer, _default_model, _load_error
    if _infer is not None:
        return _infer, _default_model
    if _load_error is not None:
        raise RuntimeError(_load_error)

    try:
        from f5_tts.infer.infer_gradio import DEFAULT_TTS_MODEL, infer

        _infer = infer
        _default_model = DEFAULT_TTS_MODEL
        return _infer, _default_model
    except Exception as exc:
        _load_error = str(exc)
        raise


def auto_reference_text(ref_audio: str, language: str):
    if not ref_audio:
        return "", "Chưa có audio mẫu."
    try:
        text = transcribe_with_gemini(ref_audio, language)
        return text, "Đã lấy text từ audio bằng Gemini."
    except Exception as exc:
        return "", str(exc)


def generate(ref_audio: str, ref_text: str, target_text: str, language: str):
    if not ref_audio:
        return None, ref_text, "Chưa có audio mẫu."
    if not target_text or not target_text.strip():
        return None, ref_text, "Chưa có câu cần đọc."

    current_ref_text = (ref_text or "").strip()
    if not current_ref_text:
        try:
            current_ref_text = transcribe_with_gemini(ref_audio, language)
        except Exception as exc:
            return None, ref_text, f"Không tự lấy được text: {exc}"

    try:
        infer_fn, model_name = ensure_f5_loaded()
        audio_out, _spectrogram, ref_text_out, _seed = infer_fn(
            ref_audio,
            current_ref_text,
            target_text.strip(),
            model_name,
            False,
            0,
            speed=1.0,
            show_info=lambda _msg: None,
        )
        return audio_out, ref_text_out, "Xong."
    except Exception as exc:
        return None, current_ref_text or ref_text, f"Lỗi: {exc}"


def build_demo() -> gr.Blocks:
    with gr.Blocks(title="F5 Clone") as demo:
        gr.Markdown("# F5 Clone")
        with gr.Row():
            with gr.Column():
                ref_audio = gr.Audio(label="Audio mẫu", type="filepath")
                ref_text = gr.Textbox(label="Text mẫu", lines=3, placeholder="Để trống rồi bấm 'Lấy text bằng Gemini' hoặc cứ Generate.")
                auto_btn = gr.Button("Lấy text bằng Gemini")
            with gr.Column():
                target_text = gr.Textbox(label="Text cần đọc", lines=5)
                language = gr.Dropdown(["English", "Vietnamese", "Auto"], value="English", label="Ngôn ngữ")
                generate_btn = gr.Button("Tạo audio", variant="primary")
                status = gr.Textbox(label="Trạng thái", lines=2)
                audio_out = gr.Audio(label="Kết quả", type="numpy")

        auto_btn.click(
            auto_reference_text,
            inputs=[ref_audio, language],
            outputs=[ref_text, status],
        )
        generate_btn.click(
            generate,
            inputs=[ref_audio, ref_text, target_text, language],
            outputs=[audio_out, ref_text, status],
        )
    return demo


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7860)
    args = parser.parse_args()

    demo = build_demo()
    demo.queue(default_concurrency_limit=2).launch(
        server_name=args.host,
        server_port=args.port,
        share=False,
        theme=gr.themes.Soft(),
    )


if __name__ == "__main__":
    main()
