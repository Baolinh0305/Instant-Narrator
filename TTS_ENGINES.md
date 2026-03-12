# TTS Engines

## F5-TTS

- Chay: `start_f5_tts_gpu.bat`
- Web UI: `http://127.0.0.1:7860`
- UI da duoc rut gon.
- Co nut `Lay text bang Gemini` de tu dong dien `Text mau`.
- Script da tu them FFmpeg shared vao `PATH`.

## Qwen3-TTS

- Chay: `start_qwen3_tts_gpu.bat`
- Web UI: `http://127.0.0.1:7861`
- Script mac dinh dung model `Qwen/Qwen3-TTS-12Hz-1.7B-Base`.
- Lan dau chay se tu tai model tu Hugging Face.
- Dang chay GPU `cuda:0`, `float16`, `--no-flash-attn`.
- Neu bat file `.bat` ma cua so tu tat, script moi se giu lai console neu co loi de de xem traceback.
- UI da duoc rut gon.
- Co nut `Lay text bang Gemini` de tu dong dien `Text mau`.
- `Use x-vector only`: nhanh hon, khong can text mau, nhung do giong va nhip noi thuong kem hon.

## Ghi chu

- Ca hai moi truong da cai rieng trong `engines\`.
- Qwen3-TTS can `SoX`; script da tu them vao `PATH`.
- Model/cache moi se duoc luu trong `models-cache\` ngay tai thu muc du an, khong dung cache mac dinh o `C:\Users\ngbal\.cache\huggingface`.
