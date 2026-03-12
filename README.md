# Instant Gemini Live TTS (Rust)

App desktop Rust nhẹ để nhập text, prompt phong cách đọc, chọn voice Gemini, preview voice và xuất WAV.

## Tinh nang moi

- Keo tha file audio ngan de Gemini phan tich transcript + cau truc giong.
- Sinh `tts_prompt` de copy thang vao o `Prompt giong dieu / cach doc`.
- Co nut `Dung prompt nay cho TTS` de nap prompt phan tich vao app ngay.
- Co san `F5-TTS` va `Qwen3-TTS Base` chay GPU bang script `.bat`.

## Yêu cầu

- Rust toolchain (stable)
- Gemini API key có quyền model native-audio

## Chạy app

1. (Khuyến nghị) set API key trước:

```powershell
$env:GEMINI_API_KEY="YOUR_API_KEY"
```

2. Chạy:

```powershell
cargo run
```

## Cách dùng

1. `Gemini API Key` đã được điền mặc định sẵn trong app (vẫn có thể sửa tay nếu cần).
2. Chọn tốc độ đọc, nhập prompt giọng điệu.
3. Nhập text preview hoặc text chính.
4. Chọn voice trong danh sách Nam/Nữ, app sẽ preview ngay bằng prompt của bạn.
5. Bấm `Đọc văn bản` để đọc text chính.
6. Bấm `Xuất audio (WAV)` để lưu mặc định vào `D:\audio`.
7. Bấm `Dừng` để ngắt ngay.

## Ghi chú

- Gemini Live TTS bắt buộc cần API key.
- Phan tich audio dung Gemini `generateContent` voi audio inline, hop hon cho clip ngan.
- App dùng model: `gemini-2.5-flash-native-audio-preview-12-2025`.
- Audio nhận về PCM 24kHz mono và phát trên default output device của Windows.
- Xem `TTS_ENGINES.md` de mo nhanh `F5-TTS` va `Qwen3-TTS Base`.
