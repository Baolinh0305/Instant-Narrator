# TTS Engines

## Qwen3-TTS

- Run: `start_qwen3_tts_gpu.bat`
- Web UI: `http://127.0.0.1:7861`
- Default model: `Qwen/Qwen3-TTS-12Hz-1.7B-Base`
- First run downloads the model from Hugging Face.
- Runs on GPU `cuda:0`, `float16`, `--no-flash-attn`.
- If the `.bat` window closes on error, the script keeps the console open so traceback stays visible.
- The UI is reduced and includes Gemini-assisted reference text filling.
- `Use x-vector only` is faster and does not require reference text, but usually gives weaker similarity and prosody.

## Notes

- The Qwen environment is installed under `engines\`.
- Qwen3-TTS requires `SoX`; the script appends it to `PATH`.
- Model/cache files are stored under `models-cache\` inside the project, not under the default Hugging Face cache in `C:\Users\ngbal\.cache\huggingface`.
