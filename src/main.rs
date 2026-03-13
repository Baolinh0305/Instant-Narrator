use std::fs;
use std::io::{BufRead, BufReader};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Mutex;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose};
use eframe::egui;
use image::ImageReader;
use native_tls::{TlsConnector, TlsStream};
use rodio::{OutputStream, OutputStreamBuilder, Sink, buffer::SamplesBuffer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tungstenite::{Message, WebSocket, client::client};

const TTS_MODEL: &str = "gemini-2.5-flash-native-audio-preview-12-2025";
const ANALYSIS_MODEL: &str = "gemini-2.5-flash";
const SAMPLE_RATE: u32 = 24_000;
const MAX_INLINE_AUDIO_BYTES: usize = 18 * 1024 * 1024;
const DEFAULT_VOICE: &str = "Aoede";
const DEFAULT_PREVIEW_TEXT: &str = "Hello, this is a Gemini Live voice preview.";
const EXPORT_DIR: &str = r"D:\audio";
const APP_DATA_DIR: &str = "app-data";
const QWEN_PORT: u16 = 7861;
const QWEN_SERVICE_URLS: &[&str] = &["http://127.0.0.1:7862"];
const QWEN_SERVICE_PORTS: &[u16] = &[7862];
const QWEN_EXPORT_WORKERS: usize = 1;
const GEMINI_AUDIO_RETRIES: usize = 2;
const BOOK_GEMINI_RETRIES: usize = 2;
const BOOK_GEMINI_SPACING_MS: u64 = 10_000;
const GEMINI_TURN_TIMEOUT_SECS: u64 = 45;
const GEMINI_SHORT_LINE_THRESHOLD: usize = 28;
const GEMINI_LONG_LINE_SPLIT_THRESHOLD: usize = 380;
const GROUPED_GEMINI_MAX_CHARS: usize = 420;
const GROUPED_QWEN_MAX_CHARS: usize = 700;
const QWEN_SERVICE_RETRIES: usize = 2;
const QWEN_RESTART_COOLDOWN_SECS: u64 = 8;
const QWEN_REQUEST_TIMEOUT_SECS: u64 = 120;
const QWEN_SUPPORTED_LANGUAGES: &[&str] = &[
    "Auto",
    "Chinese",
    "English",
    "Japanese",
    "Korean",
    "German",
    "French",
    "Russian",
    "Portuguese",
    "Spanish",
    "Italian",
];
const VIDEO_FONT_OPTIONS: &[&str] = &[
    "Tahoma",
    "Segoe UI",
    "Arial",
    "Verdana",
    "Times New Roman",
    "Georgia",
    "Trebuchet MS",
    "Consolas",
];
const DEFAULT_VIDEO_FONT_SIZE: u32 = 60;
const DEFAULT_VIDEO_SUBTITLE_LEAD_SECONDS: f32 = 0.0;

const MALE_VOICES: &[&str] = &[
    "Achird",
    "Algenib",
    "Algieba",
    "Alnilam",
    "Charon",
    "Enceladus",
    "Fenrir",
    "Iapetus",
    "Orus",
    "Puck",
    "Rasalgethi",
    "Sadachbia",
    "Sadaltager",
    "Schedar",
    "Umbriel",
    "Zubenelgenubi",
];

const FEMALE_VOICES: &[&str] = &[
    "Achernar",
    "Aoede",
    "Autonoe",
    "Callirrhoe",
    "Despina",
    "Erinome",
    "Gacrux",
    "Kore",
    "Laomedeia",
    "Leda",
    "Pulcherrima",
    "Sulafat",
    "Vindemiatrix",
    "Zephyr",
];

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SpeechSpeed {
    Slow,
    Normal,
    Fast,
}

impl SpeechSpeed {
    fn as_str(self) -> &'static str {
        match self {
            Self::Slow => "Ch?m",
            Self::Normal => "B?nh th??ng",
            Self::Fast => "Nhanh",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum ExportRenderMode {
    PerLine,
    ByCharacter,
}

impl ExportRenderMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::PerLine => "Theo t?ng d?ng",
            Self::ByCharacter => "G?p theo nh?n v?t",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SpeakerExportMode {
    PerLine,
    Grouped,
    Exclude,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum VideoResolution {
    Hd720,
    FullHd1080,
}

impl VideoResolution {
    fn label(self) -> &'static str {
        match self {
            Self::Hd720 => "720p",
            Self::FullHd1080 => "1080p",
        }
    }

    fn dimensions(self) -> (u32, u32) {
        match self {
            Self::Hd720 => (1280, 720),
            Self::FullHd1080 => (1920, 1080),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum VideoFrameRate {
    Fps30,
    Fps60,
}

impl VideoFrameRate {
    fn label(self) -> &'static str {
        match self {
            Self::Fps30 => "30 fps",
            Self::Fps60 => "60 fps",
        }
    }

    fn value(self) -> u32 {
        match self {
            Self::Fps30 => 30,
            Self::Fps60 => 60,
        }
    }
}

#[derive(Clone)]
struct TtsJob {
    api_key: String,
    text: String,
    style_instruction: String,
    voice_name: String,
    speed: SpeechSpeed,
}

#[derive(Clone)]
struct AnalysisJob {
    api_key: String,
    audio_path: PathBuf,
}

#[derive(Clone)]
struct BookFormatJob {
    api_key: String,
    source_text: String,
}

#[derive(Clone)]
struct CharacterAnalysisJob {
    api_key: String,
    source_text: String,
}

#[derive(Clone)]
struct BookLine {
    speaker: String,
    text: String,
}

#[derive(Clone)]
struct TimedBookLine {
    speaker: String,
    text: String,
    start_seconds: f64,
    end_seconds: f64,
}

#[derive(Clone, Serialize, Deserialize)]
struct TimedWordSegment {
    line_index: usize,
    word_index: usize,
    word: String,
    start_seconds: f64,
    end_seconds: f64,
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct ExportedSpeech {
    index: usize,
    speaker: String,
    text: String,
    audio_path: String,
    #[serde(default = "default_volume_percent")]
    applied_gain_percent: u32,
}

#[derive(Clone)]
struct ExportTask {
    index: usize,
    line: BookLine,
    character: CharacterRecord,
    volume_percent: u32,
    output_path: PathBuf,
    temp_dir: PathBuf,
    engine: ExportEngine,
}

#[derive(Clone)]
struct GroupedExportTask {
    speaker: String,
    character: CharacterRecord,
    engine: ExportEngine,
    tasks: Vec<ExportTask>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExportEngine {
    Qwen,
    Gemini,
}

struct ExportTaskResult {
    index: usize,
    speaker: String,
    text: String,
    applied_gain_percent: u32,
    output_path: Option<PathBuf>,
    ref_text_update: Option<(String, String)>,
    error: Option<String>,
    engine: ExportEngine,
}

#[derive(Clone, Default)]
struct VoiceAnalysis {
    transcript: String,
    style_summary: String,
    tts_prompt: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
struct CharacterRecord {
    name: String,
    description: String,
    ref_text: String,
    ref_audio_path: String,
    tts_engine: String,
    gemini_voice: String,
    gemini_style_prompt: String,
    gemini_speed: SpeechSpeed,
    #[serde(default = "default_volume_percent")]
    volume_percent: u32,
}

impl Default for CharacterRecord {
    fn default() -> Self {
        Self {
            name: String::new(),
            description: String::new(),
            ref_text: String::new(),
            ref_audio_path: String::new(),
            tts_engine: "qwen".to_string(),
            gemini_voice: DEFAULT_VOICE.to_string(),
            gemini_style_prompt: String::new(),
            gemini_speed: SpeechSpeed::Normal,
            volume_percent: 100,
        }
    }
}

fn default_volume_percent() -> u32 {
    100
}

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct LineVolumeSetting {
    index: usize,
    gain_percent: u32,
}

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct SpeakerExportSetting {
    speaker: String,
    mode: SpeakerExportMode,
    voice_character: String,
}

impl Default for SpeakerExportMode {
    fn default() -> Self {
        Self::PerLine
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
struct PersistedState {
    api_key: String,
    book_input: String,
    book_output: String,
    characters: Vec<CharacterRecord>,
    audiobook_pause_ms: u32,
    exported_speeches: Vec<ExportedSpeech>,
    line_volume_settings: Vec<LineVolumeSetting>,
    export_render_mode: ExportRenderMode,
    export_excluded_characters: Vec<String>,
    export_speaker_settings: Vec<SpeakerExportSetting>,
    auto_merge_after_render: bool,
    video_background_path: String,
    video_corner_tag: String,
    video_tag_position: VideoTagPosition,
    video_tag_font_size: u32,
    video_tag_background_enabled: bool,
    video_font_name: String,
    video_text_color: String,
    video_card_opacity: u8,
    video_font_size: u32,
    video_resolution: VideoResolution,
    video_frame_rate: VideoFrameRate,
    last_export_dir: String,
    last_audiobook_path: String,
    video_srt_path: String,
    video_preview_audio_path: String,
    video_preview_clip_duration_seconds: u32,
    video_word_highlight_enabled: bool,
    video_subtitle_lead_seconds: f32,
}

impl Default for PersistedState {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            book_input: String::new(),
            book_output: String::new(),
            characters: Vec::new(),
            audiobook_pause_ms: 320,
            exported_speeches: Vec::new(),
            line_volume_settings: Vec::new(),
            export_render_mode: ExportRenderMode::PerLine,
            export_excluded_characters: Vec::new(),
            export_speaker_settings: Vec::new(),
            auto_merge_after_render: true,
            video_background_path: String::new(),
            video_corner_tag: String::new(),
            video_tag_position: VideoTagPosition::TopLeft,
            video_tag_font_size: 70,
            video_tag_background_enabled: false,
            video_font_name: "Tahoma".to_string(),
            video_text_color: "#FFFFFF".to_string(),
            video_card_opacity: 185,
            video_font_size: DEFAULT_VIDEO_FONT_SIZE,
            video_resolution: VideoResolution::Hd720,
            video_frame_rate: VideoFrameRate::Fps30,
            last_export_dir: String::new(),
            last_audiobook_path: String::new(),
            video_srt_path: String::new(),
            video_preview_audio_path: String::new(),
            video_preview_clip_duration_seconds: 12,
            video_word_highlight_enabled: true,
            video_subtitle_lead_seconds: DEFAULT_VIDEO_SUBTITLE_LEAD_SECONDS,
        }
    }
}

enum TtsEvent {
    Status(String),
    Audio(Vec<i16>),
    Error(String),
}

enum AnalysisEvent {
    Status(String),
    Completed(VoiceAnalysis),
    Error(String),
}

enum BookEvent {
    Status(String),
    Completed(String),
    Error(String),
}

enum CharacterEvent {
    Status(String),
    Completed(Vec<CharacterRecord>),
    Error(String),
}

enum VideoEvent {
    Status(String),
    Progress {
        fraction: f32,
        label: String,
    },
    SrtReady {
        path: PathBuf,
        timed_lines: Vec<TimedBookLine>,
        word_segments: Vec<TimedWordSegment>,
    },
    Done(PathBuf),
    Cancelled(String),
    Error(String),
}

enum SrtJobEvent {
    Status {
        job_id: u64,
        message: String,
    },
    Progress {
        job_id: u64,
        fraction: f32,
        label: String,
    },
    Ready {
        job_id: u64,
        project_root: String,
        audio_path: String,
        path: PathBuf,
        timed_lines: Vec<TimedBookLine>,
        word_segments: Vec<TimedWordSegment>,
    },
    Error {
        job_id: u64,
        error: String,
    },
    Cancelled {
        job_id: u64,
        message: String,
    },
}

struct SrtBackgroundJob {
    id: u64,
    project_root: String,
    audio_path: String,
    status: String,
    progress_fraction: f32,
    progress_label: String,
    srt_path: String,
    finished: bool,
    failed: bool,
}

enum QwenEvent {
    Status(String),
    ExportEngineStatus {
        engine: ExportEngine,
        message: String,
    },
    ExportProgress {
        qwen_done: usize,
        qwen_total: usize,
        gemini_done: usize,
        gemini_total: usize,
        created: usize,
        skipped: usize,
    },
    RefTextReady {
        character_name: Option<String>,
        text: String,
    },
    PreviewReady {
        character_name: Option<String>,
        ref_text: Option<String>,
        samples: Vec<i16>,
        message: String,
    },
    LineRendered {
        speech: ExportedSpeech,
        message: String,
    },
    SpeechReady {
        speech: ExportedSpeech,
    },
    LineSkipped {
        index: usize,
        speaker: String,
        reason: String,
    },
    CharacterBatchDone {
        speaker: String,
        rendered: usize,
        skipped: usize,
        engine: ExportEngine,
    },
    ExportDone {
        output_dir: String,
        created: usize,
        skipped: usize,
        updates: Vec<(String, String)>,
        audiobook_path: Option<String>,
        merge_error: Option<String>,
        speeches: Vec<ExportedSpeech>,
    },
    Error(String),
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum VideoTagPosition {
    TopLeft,
    TopCenter,
}

enum LineRenderEvent {
    Status {
        engine: ExportEngine,
        index: usize,
        message: String,
    },
    Rendered {
        engine: ExportEngine,
        speech: ExportedSpeech,
        message: String,
    },
    Error {
        engine: ExportEngine,
        index: usize,
        error: String,
    },
}

struct TtsApp {
    api_key: String,
    text: String,
    preview_text: String,
    style_instruction: String,
    voice_name: String,
    speed: SpeechSpeed,
    status: String,
    analysis_status: String,
    book_status: String,
    export_status: String,
    export_qwen_status: String,
    export_gemini_status: String,
    character_status: String,
    is_fetching: bool,
    is_playing: bool,
    is_analyzing: bool,
    is_formatting_book: bool,
    is_analyzing_characters: bool,
    tts_rx: Option<Receiver<TtsEvent>>,
    analysis_rx: Option<Receiver<AnalysisEvent>>,
    book_rx: Option<Receiver<BookEvent>>,
    character_rx: Option<Receiver<CharacterEvent>>,
    qwen_rx: Option<Receiver<QwenEvent>>,
    line_render_tx: Sender<LineRenderEvent>,
    line_render_rx: Receiver<LineRenderEvent>,
    cancel_flag: Arc<AtomicBool>,
    stream: Option<OutputStream>,
    sink: Option<Sink>,
    last_audio_samples: Option<Vec<i16>>,
    dropped_audio_path: Option<PathBuf>,
    analysis_result: VoiceAnalysis,
    book_input: String,
    book_output: String,
    characters: Vec<CharacterRecord>,
    selected_character: Option<usize>,
    character_name_input: String,
    character_description_input: String,
    character_ref_text_input: String,
    character_ref_audio_input: String,
    character_tts_engine_input: String,
    character_gemini_voice_input: String,
    character_gemini_style_prompt_input: String,
    character_gemini_speed_input: SpeechSpeed,
    show_book_source: bool,
    show_book_result: bool,
    show_analysis_panel: bool,
    qwen_status: String,
    is_qwen_busy: bool,
    active_qwen_line_renders: usize,
    active_gemini_line_renders: usize,
    is_exporting_book: bool,
    qwen_service_ready: bool,
    qwen_model_ready: bool,
    qwen_service_healths: Vec<(bool, bool)>,
    qwen_language: String,
    qwen_xvector_only: bool,
    selected_book_line: Option<usize>,
    last_export_dir: String,
    last_audiobook_path: String,
    audiobook_pause_ms: u32,
    exported_speeches: Vec<ExportedSpeech>,
    export_skip_details: Vec<String>,
    export_skipped_indices: Vec<usize>,
    line_volume_settings: Vec<LineVolumeSetting>,
    export_render_mode: ExportRenderMode,
    export_excluded_characters: Vec<String>,
    export_speaker_settings: Vec<SpeakerExportSetting>,
    show_export_exclusion_picker: bool,
    show_official_export_settings: bool,
    show_missing_audio_lines: bool,
    auto_merge_after_render: bool,
    export_progress_done: usize,
    export_progress_total: usize,
    export_progress_label: String,
    qwen_ready: bool,
    last_engine_probe: Instant,
    show_video_window: bool,
    is_exporting_video: bool,
    video_status: String,
    video_background_path: String,
    video_corner_tag: String,
    video_tag_position: VideoTagPosition,
    video_tag_font_size: u32,
    video_tag_background_enabled: bool,
    video_font_name: String,
    video_text_color: String,
    video_card_opacity: u8,
    video_font_size: u32,
    video_resolution: VideoResolution,
    video_frame_rate: VideoFrameRate,
    video_rx: Option<Receiver<VideoEvent>>,
    video_cancel_flag: Arc<AtomicBool>,
    video_preview_texture: Option<egui::TextureHandle>,
    video_preview_texture_path: String,
    video_preview_line_index: usize,
    video_progress_fraction: f32,
    video_progress_label: String,
    video_timed_lines: Vec<TimedBookLine>,
    video_word_segments: Vec<TimedWordSegment>,
    video_word_highlight_enabled: bool,
    video_subtitle_lead_seconds: f32,
    video_srt_path: String,
    video_preview_started_at: Option<Instant>,
    video_preview_audio_path: String,
    video_preview_seek_seconds: f64,
    last_video_export_path: String,
    video_preview_clip_duration_seconds: u32,
    srt_jobs: Vec<SrtBackgroundJob>,
    srt_job_tx: Sender<SrtJobEvent>,
    srt_job_rx: Receiver<SrtJobEvent>,
    srt_job_cancel_flags: HashMap<u64, Arc<AtomicBool>>,
    next_srt_job_id: u64,
    show_project_picker: bool,
    project_picker_selected: String,
    show_project_rename: bool,
    project_rename_input: String,
}

impl Default for TtsApp {
    fn default() -> Self {
        let persisted = load_persisted_state().unwrap_or_default();
        let (srt_job_tx, srt_job_rx) = mpsc::channel();
        let (line_render_tx, line_render_rx) = mpsc::channel();
        let initial_api_key = if persisted.api_key.trim().is_empty() {
            std::env::var("GEMINI_API_KEY").unwrap_or_default()
        } else {
            persisted.api_key.clone()
        };
        let mut app = Self {
            api_key: initial_api_key,
            text: String::new(),
            preview_text: DEFAULT_PREVIEW_TEXT.to_string(),
            style_instruction: "Read naturally, clearly, and with the right emotional context.".to_string(),
            voice_name: DEFAULT_VOICE.to_string(),
            speed: SpeechSpeed::Normal,
            status: "S?n s�ng.".to_string(),
            analysis_status: "Drag and drop an audio file into the analysis panel to begin.".to_string(),
            book_status: "Paste your story text here, then click process.".to_string(),
            export_status: "Chua xu?t audio book.".to_string(),
            export_qwen_status: "Qwen chua xu?t g�.".to_string(),
            export_gemini_status: "Gemini chua xu?t g�.".to_string(),
            character_status: "No character selected.".to_string(),
            is_fetching: false,
            is_playing: false,
            is_analyzing: false,
            is_formatting_book: false,
            is_analyzing_characters: false,
            tts_rx: None,
            analysis_rx: None,
            book_rx: None,
            character_rx: None,
            qwen_rx: None,
            line_render_tx,
            line_render_rx,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            stream: None,
            sink: None,
            last_audio_samples: None,
            dropped_audio_path: None,
            analysis_result: VoiceAnalysis::default(),
            book_input: persisted.book_input,
            book_output: persisted.book_output,
            characters: persisted.characters,
            selected_character: None,
            character_name_input: String::new(),
            character_description_input: String::new(),
            character_ref_text_input: String::new(),
            character_ref_audio_input: String::new(),
            character_tts_engine_input: "qwen".to_string(),
            character_gemini_voice_input: DEFAULT_VOICE.to_string(),
            character_gemini_style_prompt_input: String::new(),
            character_gemini_speed_input: SpeechSpeed::Normal,
            show_book_source: false,
            show_book_result: false,
            show_analysis_panel: false,
            qwen_status: "Qwen is not ready yet.".to_string(),
            is_qwen_busy: false,
            active_qwen_line_renders: 0,
            active_gemini_line_renders: 0,
            is_exporting_book: false,
            qwen_service_ready: false,
            qwen_model_ready: false,
            qwen_service_healths: vec![(false, false); QWEN_SERVICE_URLS.len()],
            qwen_language: "English".to_string(),
            qwen_xvector_only: false,
            selected_book_line: None,
            last_export_dir: if persisted.last_export_dir.trim().is_empty() {
                String::new()
            } else {
                normalize_project_root(Path::new(&persisted.last_export_dir))
                    .to_string_lossy()
                    .to_string()
            },
            last_audiobook_path: persisted.last_audiobook_path,
            audiobook_pause_ms: persisted.audiobook_pause_ms.max(120),
            exported_speeches: persisted.exported_speeches,
            export_skip_details: Vec::new(),
            export_skipped_indices: Vec::new(),
            line_volume_settings: persisted.line_volume_settings,
            export_render_mode: persisted.export_render_mode,
            export_excluded_characters: persisted.export_excluded_characters,
            export_speaker_settings: persisted.export_speaker_settings,
            show_export_exclusion_picker: false,
            show_official_export_settings: false,
            show_missing_audio_lines: false,
            auto_merge_after_render: persisted.auto_merge_after_render,
            export_progress_done: 0,
            export_progress_total: 0,
            export_progress_label: String::new(),
            qwen_ready: false,
            last_engine_probe: Instant::now() - Duration::from_secs(10),
            show_video_window: false,
            is_exporting_video: false,
            video_status: "No video exported yet.".to_string(),
            video_background_path: persisted.video_background_path,
            video_corner_tag: persisted.video_corner_tag,
            video_tag_position: persisted.video_tag_position,
            video_tag_font_size: if persisted.video_tag_font_size == 0 {
                70
            } else {
                persisted.video_tag_font_size.clamp(28, 120)
            },
            video_tag_background_enabled: persisted.video_tag_background_enabled,
            video_font_name: if persisted.video_font_name.trim().is_empty() {
                "Tahoma".to_string()
            } else {
                persisted.video_font_name
            },
            video_text_color: if persisted.video_text_color.trim().is_empty() {
                "#FFFFFF".to_string()
            } else {
                persisted.video_text_color
            },
            video_card_opacity: persisted.video_card_opacity.clamp(40, 240),
            video_font_size: if persisted.video_font_size == 0 {
                DEFAULT_VIDEO_FONT_SIZE
            } else {
                persisted.video_font_size.clamp(20, 96)
            },
            video_resolution: persisted.video_resolution,
            video_frame_rate: persisted.video_frame_rate,
            video_rx: None,
            video_cancel_flag: Arc::new(AtomicBool::new(false)),
            video_preview_texture: None,
            video_preview_texture_path: String::new(),
            video_preview_line_index: 0,
            video_progress_fraction: 0.0,
            video_progress_label: String::new(),
            video_timed_lines: Vec::new(),
            video_word_segments: Vec::new(),
            video_word_highlight_enabled: persisted.video_word_highlight_enabled,
            video_subtitle_lead_seconds: persisted
                .video_subtitle_lead_seconds
                .clamp(0.0, 1.5),
            video_srt_path: persisted.video_srt_path,
            video_preview_started_at: None,
            video_preview_audio_path: persisted.video_preview_audio_path,
            video_preview_seek_seconds: 0.0,
            last_video_export_path: String::new(),
            video_preview_clip_duration_seconds: persisted
                .video_preview_clip_duration_seconds
                .clamp(3, 90),
            srt_jobs: Vec::new(),
            srt_job_tx,
            srt_job_rx,
            srt_job_cancel_flags: HashMap::new(),
            next_srt_job_id: 1,
            show_project_picker: false,
            project_picker_selected: if persisted.last_export_dir.trim().is_empty() {
                String::new()
            } else {
                normalize_project_root(Path::new(&persisted.last_export_dir))
                    .to_string_lossy()
                    .to_string()
            },
            show_project_rename: false,
            project_rename_input: String::new(),
        };

        let persisted_project = if persisted.last_export_dir.trim().is_empty() {
            None
        } else {
            let path = normalize_project_root(Path::new(&persisted.last_export_dir));
            path.exists().then_some(path)
        };

        let default_project = persisted_project.or_else(|| {
            list_available_project_dirs()
                .ok()
                .and_then(|projects| projects.into_iter().last())
        });

        match default_project
            .map(Ok)
            .unwrap_or_else(|| next_project_root_dir())
            .and_then(|dir| {
                ensure_project_structure(&dir)?;
                app.load_project(&dir)
            })
        {
            Ok(()) => {}
            Err(err) => {
                app.show_project_picker = true;
                app.status = format!("Could not open the default project: {}", err);
            }
        }

        app
    }
}

impl TtsApp {
    fn poll_line_render_events(&mut self) {
        loop {
            match self.line_render_rx.try_recv() {
                Ok(event) => match event {
                    LineRenderEvent::Status {
                        engine,
                        index,
                        message,
                    } => {
                        self.export_status = message.clone();
                        match engine {
                            ExportEngine::Qwen => {
                                self.export_qwen_status =
                                    format!("Rendering line {} with Qwen...", index + 1);
                                self.qwen_status = self.export_qwen_status.clone();
                            }
                            ExportEngine::Gemini => {
                                self.export_gemini_status =
                                    format!("Rendering line {} with Gemini...", index + 1);
                                self.qwen_status = self.export_gemini_status.clone();
                            }
                        }
                    }
                    LineRenderEvent::Rendered {
                        engine,
                        speech,
                        message,
                    } => {
                        self.exported_speeches.retain(|item| item.index != speech.index);
                        self.exported_speeches.push(speech.clone());
                        self.exported_speeches.sort_by_key(|item| item.index);
                        self.export_skipped_indices.retain(|item| *item != speech.index);
                        self.export_skip_details.retain(|detail| {
                            !detail.contains(&format!("line {}", speech.index + 1))
                                && !detail.contains(&format!("Dòng {}", speech.index + 1))
                        });
                        self.export_status = message.clone();
                        match engine {
                            ExportEngine::Qwen => {
                                self.active_qwen_line_renders =
                                    self.active_qwen_line_renders.saturating_sub(1);
                                self.export_qwen_status = message.clone();
                                self.qwen_status = self.export_qwen_status.clone();
                            }
                            ExportEngine::Gemini => {
                                self.active_gemini_line_renders =
                                    self.active_gemini_line_renders.saturating_sub(1);
                                self.export_gemini_status = message.clone();
                                self.qwen_status = self.export_gemini_status.clone();
                            }
                        }
                        let _ = self.persist_state_to_disk();
                    }
                    LineRenderEvent::Error {
                        engine,
                        index,
                        error,
                    } => {
                        self.export_status = format!("Render line failed: {}", error);
                        match engine {
                            ExportEngine::Qwen => {
                                self.active_qwen_line_renders =
                                    self.active_qwen_line_renders.saturating_sub(1);
                                self.export_qwen_status =
                                    format!("Line {} failed: {}", index + 1, error);
                                self.qwen_status = self.export_qwen_status.clone();
                            }
                            ExportEngine::Gemini => {
                                self.active_gemini_line_renders =
                                    self.active_gemini_line_renders.saturating_sub(1);
                                self.export_gemini_status =
                                    format!("Line {} failed: {}", index + 1, error);
                                self.qwen_status = self.export_gemini_status.clone();
                            }
                        }
                    }
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }

    fn handle_dropped_files(&mut self, ctx: &egui::Context) {
        let dropped_files = ctx.input(|i| i.raw.dropped_files.clone());

        for file in dropped_files {
            if let Some(path) = file.path {
                if infer_audio_mime_type(&path).is_some() {
                    self.dropped_audio_path = Some(path.clone());
                    self.character_ref_audio_input = path.to_string_lossy().to_string();
                    self.analysis_status =
                        format!("�� nh?n file audio: {}", path.to_string_lossy());
                    self.character_status =
                        "�� nh?n audio m?u v� g�n v�o nh�n v?t hi?n t?i.".to_string();
                } else if is_supported_srt_file(&path) {
                    match self.load_video_srt_from_path(&path) {
                        Ok(()) => {
                            self.show_video_window = true;
                        }
                        Err(err) => {
                            self.video_status = format!("Kh�ng n?p du?c SRT: {}", err);
                        }
                    }
                } else if is_supported_image_file(&path) {
                    self.video_background_path = path.to_string_lossy().to_string();
                    self.video_status =
                        format!("�� nh?n ?nh n?n video: {}", path.to_string_lossy());
                    self.video_preview_texture = None;
                    self.video_preview_texture_path.clear();
                    let _ = self.persist_state_to_disk();
                } else {
                    self.analysis_status = format!(
                        "File kh�ng du?c h? tr?: {}. D�ng mp3, wav, m4a, flac, ogg, webm, aac.",
                        path.to_string_lossy()
                    );
                }
            }
        }
    }

    fn clear_speech_cache(&mut self) {
        self.exported_speeches.clear();
        self.export_skip_details.clear();
        self.last_audiobook_path.clear();
        let cache_dir = app_data_dir().join("speech-cache").join("latest");
        let _ = fs::remove_dir_all(&cache_dir);
        let _ = fs::create_dir_all(&cache_dir);
        let _ = self.persist_state_to_disk();
    }

    fn can_export_video(&self) -> bool {
        !parse_book_lines(&self.book_output).is_empty() && self.missing_cached_book_line_indices().is_empty()
    }

    fn set_book_line_speaker(&mut self, index: usize, speaker: &str) {
        let mut lines = parse_book_lines(&self.book_output);
        let Some(line) = lines.get_mut(index) else {
            return;
        };
        let normalized = normalize_speaker_alias(speaker.trim());
        if normalized.is_empty() || normalized.eq_ignore_ascii_case(&line.speaker) {
            return;
        }

        line.speaker = normalized.clone();
        self.book_output = serialize_book_lines(&lines);
        self.exported_speeches.clear();
        self.export_skip_details.clear();
        self.last_audiobook_path.clear();
        self.selected_book_line = Some(index);
        self.export_status = format!(
            "�� d?i speaker d�ng {} th�nh '{}'. Cache audio cu d� b? x�a d? tr�nh l?ch th? t?, c?n render l?i.",
            index + 1,
            normalized
        );
        let _ = self.persist_state_to_disk();
    }

    fn set_book_line_text(&mut self, index: usize, text: &str) {
        let mut lines = parse_book_lines(&self.book_output);
        let Some(line) = lines.get_mut(index) else {
            return;
        };
        let normalized = text.trim().to_string();
        if normalized.is_empty() || normalized == line.text {
            return;
        }

        line.text = normalized.clone();
        self.book_output = serialize_book_lines(&lines);
        self.exported_speeches.retain(|item| item.index != index);
        self.export_skip_details
            .retain(|item| !item.starts_with(&format!("D?ng {} ", index + 1)));
        self.export_skipped_indices.retain(|item| *item != index);
        let cache_path = app_data_dir()
            .join("speech-cache")
            .join("latest")
            .join(format!("{:04}.wav", index + 1));
        let _ = fs::remove_file(cache_path);
        self.last_audiobook_path.clear();
        self.video_srt_path.clear();
        self.video_timed_lines.clear();
        self.video_word_segments.clear();
        self.last_video_export_path.clear();
        self.selected_book_line = Some(index);
        self.export_status = format!(
            "Updated text for line {}. Cached speech for this line was cleared, and the merged audiobook/subtitles must be rebuilt.",
            index + 1
        );
        let _ = self.persist_state_to_disk();
    }

    fn ensure_video_preview_texture(&mut self, ctx: &egui::Context) {
        let path = self.video_background_path.trim();
        if path.is_empty() {
            self.video_preview_texture = None;
            self.video_preview_texture_path.clear();
            return;
        }
        if self.video_preview_texture.is_some() && self.video_preview_texture_path == path {
            return;
        }

        let image = match load_color_image_from_path(Path::new(path)) {
            Ok(image) => image,
            Err(err) => {
                self.video_preview_texture = None;
                self.video_preview_texture_path.clear();
                self.video_status = format!("Kh�ng d?c du?c ?nh n?n video: {}", err);
                return;
            }
        };

        self.video_preview_texture = Some(ctx.load_texture(
            "video_preview_background",
            image,
            egui::TextureOptions::LINEAR,
        ));
        self.video_preview_texture_path = path.to_string();
    }

    fn load_video_srt_from_path(&mut self, path: &Path) -> Result<()> {
        let mut timed_lines = parse_srt_file(path)?;
        if timed_lines.is_empty() {
            return Err(anyhow!("File SRT kh�ng c� segment h?p l?."));
        }
        apply_subtitle_lead_to_lines(&mut timed_lines, self.video_subtitle_lead_seconds as f64);
        self.video_timed_lines = timed_lines;
        self.video_srt_path = path.to_string_lossy().to_string();
        let root = normalize_project_root(path);
        self.last_export_dir = root.to_string_lossy().to_string();
        if (self.video_background_path.trim().is_empty()
            || !Path::new(self.video_background_path.trim()).exists())
            && latest_background_in_project(&root).is_some()
        {
            self.video_background_path = latest_background_in_project(&root)
                .unwrap()
                .to_string_lossy()
                .to_string();
            self.video_preview_texture = None;
            self.video_preview_texture_path.clear();
        }
        let word_path = word_timing_path_for_srt(path);
        self.video_word_segments = if self.video_word_highlight_enabled && word_path.exists() {
            let mut segments = load_word_timing_file(&word_path).unwrap_or_default();
            apply_subtitle_lead_to_word_segments(
                &mut segments,
                self.video_subtitle_lead_seconds as f64,
            );
            segments
        } else {
            Vec::new()
        };
        self.video_preview_seek_seconds = 0.0;
        self.video_preview_started_at = None;
        self.video_preview_line_index = 0;
        if let Some(audio_path) = find_preview_audio_for_srt(path) {
            self.video_preview_audio_path = audio_path.to_string_lossy().to_string();
            self.video_status = format!(
                "�� n?p SRT: {}. �� t�m th?y audio preview: {}",
                path.display(),
                audio_path.display()
            );
        } else {
            self.video_preview_audio_path.clear();
            self.video_status = format!(
                "�� n?p SRT: {}. Chua t�m th?y audiobook c?nh file SRT.",
                path.display()
            );
        }
        let _ = self.persist_state_to_disk();
        Ok(())
    }

    fn preview_video_lines(&self) -> Vec<String> {
        if !self.video_timed_lines.is_empty() {
            let index = self.current_video_preview_line_index();

            let mut preview = Vec::new();
            if index >= 2 {
                preview.push(self.video_timed_lines[index - 2].text.clone());
            }
            if index >= 1 {
                preview.push(self.video_timed_lines[index - 1].text.clone());
            }
            preview.push(format!("> {}", self.video_timed_lines[index].text));
            return preview;
        }

        let lines = parse_book_lines(&self.book_output);
        if lines.is_empty() {
            return vec!["Chua c� d�ng tho?i d? preview.".to_string()];
        }

        let index = self.video_preview_line_index.min(lines.len() - 1);
        let mut preview = Vec::new();
        if index >= 2 {
            let line = &lines[index - 2];
            preview.push(line.text.clone());
        }
        if index >= 1 {
            let line = &lines[index - 1];
            preview.push(line.text.clone());
        }
        let current = &lines[index];
        preview.push(format!("> {}", current.text));
        preview
    }

    fn current_video_preview_seconds(&self) -> f64 {
        if let Some(started_at) = self.video_preview_started_at {
            self.video_preview_seek_seconds + started_at.elapsed().as_secs_f64()
        } else {
            self.video_preview_seek_seconds
        }
    }

    fn current_video_preview_line_index(&self) -> usize {
        if self.video_timed_lines.is_empty() {
            return 0;
        }
        let elapsed = self.current_video_preview_seconds();
        self.video_timed_lines
            .iter()
            .position(|line| elapsed >= line.start_seconds && elapsed < line.end_seconds)
            .unwrap_or_else(|| {
                self.video_timed_lines
                    .iter()
                    .rposition(|line| elapsed >= line.end_seconds)
                    .unwrap_or(self.video_preview_line_index.min(self.video_timed_lines.len() - 1))
            })
    }

    fn current_video_preview_word_index(&self, line_index: usize) -> Option<usize> {
        let elapsed = self.current_video_preview_seconds();
        self.video_word_segments
            .iter()
            .find(|segment| {
                segment.line_index == line_index
                    && elapsed >= segment.start_seconds
                    && elapsed < segment.end_seconds
            })
            .map(|segment| segment.word_index)
    }

    fn current_project_root(&self) -> Result<PathBuf> {
        if self.last_export_dir.trim().is_empty() {
            next_project_root_dir()
        } else {
            Ok(normalize_project_root(Path::new(&self.last_export_dir)))
        }
    }

    fn rename_current_project(&mut self, new_name: &str) -> Result<()> {
        let old_root = self.current_project_root()?;
        let parent = old_root
            .parent()
            .ok_or_else(|| anyhow!("Kh�ng x�c d?nh du?c thu m?c cha c?a project hi?n t?i."))?;
        let trimmed = new_name.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("T�n project m?i kh�ng du?c d? tr?ng."));
        }
        if trimmed.chars().any(|ch| "<>:\"/\\|?*".contains(ch)) {
            return Err(anyhow!("T�n project m?i c� k� t? kh�ng h?p l?."));
        }

        let current_name = old_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if trimmed.eq_ignore_ascii_case(current_name) {
            return Ok(());
        }

        let new_root = parent.join(trimmed);
        if new_root.exists() {
            return Err(anyhow!(
                "Project '{}' d� t?n t?i, h�y ch?n t�n kh�c.",
                trimmed
            ));
        }

        fs::rename(&old_root, &new_root).with_context(|| {
            format!(
                "Kh�ng d?i t�n du?c project {} -> {}",
                old_root.display(),
                new_root.display()
            )
        })?;
        self.load_project(&new_root)?;
        self.status = format!("�� d?i t�n project th�nh '{}'.", trimmed);
        Ok(())
    }

    fn resolve_latest_project_audiobook(&mut self, root: &Path) -> Result<PathBuf> {
        if let Some(path) = find_latest_audiobook_in_project(root) {
            self.last_audiobook_path = path.to_string_lossy().to_string();
            Ok(path)
        } else {
            self.rebuild_audiobook_from_cache()?;
            let path = PathBuf::from(self.last_audiobook_path.clone());
            if path.exists() {
                Ok(path)
            } else {
                Err(anyhow!("Kh?ng tim thay audio ?? ghep trong project hien tai."))
            }
        }
    }

    fn video_srt_matches_audio(&mut self, audio_path: &Path) -> Result<()> {
        if self.video_srt_path.trim().is_empty() {
            return Err(anyhow!("Ch?a co SRT cho audio moi nhat. Hay bam Tao SRT truoc."));
        }
        let srt_path = PathBuf::from(self.video_srt_path.trim());
        if !srt_path.exists() {
            return Err(anyhow!("File SRT hien tai khong ton tai. Hay Tao SRT lai."));
        }
        let same_project = normalize_separators(&normalize_project_root(&srt_path))
            == normalize_separators(&normalize_project_root(audio_path));
        let bound_audio = if self.video_preview_audio_path.trim().is_empty() {
            if let Some(path) = find_preview_audio_for_srt(&srt_path) {
                self.video_preview_audio_path = path.to_string_lossy().to_string();
                path
            } else if same_project {
                audio_path.to_path_buf()
            } else {
                return Err(anyhow!("SRT hien tai chua gan voi audio nao. Hay Tao SRT lai."));
            }
        } else {
            PathBuf::from(self.video_preview_audio_path.trim())
        };
        if normalize_separators(&bound_audio) != normalize_separators(audio_path) {
            return Err(anyhow!(
                "SRT hien tai ?ang thuoc audio c?. Hay Tao SRT lai cho audio moi nhat: {}",
                audio_path.display()
            ));
        }
        let srt_modified = fs::metadata(&srt_path).and_then(|meta| meta.modified()).ok();
        let audio_modified = fs::metadata(audio_path).and_then(|meta| meta.modified()).ok();
        if let (Some(srt_time), Some(audio_time)) = (srt_modified, audio_modified) {
            if srt_time < audio_time {
                return Err(anyhow!(
                    "SRT hien tai c? hon audio moi nhat. Hay Tao SRT lai truoc khi xuat video."
                ));
            }
        }
        Ok(())
    }

    fn start_video_export(&mut self) {
        if !self.can_export_video() {
            self.video_status = "Speech cache is incomplete. Video export is unavailable.".to_string();
            return;
        }
        let current_project_root = if self.last_export_dir.trim().is_empty() {
            match next_project_root_dir() {
                Ok(dir) => dir,
                Err(err) => {
                    self.video_status = format!("Could not resolve the current project: {}", err);
                    return;
                }
            }
        } else {
            normalize_project_root(Path::new(&self.last_export_dir))
        };
        let audiobook_path = match self.resolve_latest_project_audiobook(&current_project_root) {
            Ok(path) => path,
            Err(err) => {
                self.video_status = format!("Could not prepare the latest audiobook: {}", err);
                return;
            }
        };
        if let Err(err) = self.video_srt_matches_audio(&audiobook_path) {
            self.video_status = err.to_string();
            return;
        }

        let lines = parse_book_lines(&self.book_output);
        let speeches = self.exported_speeches.clone();
        let output_dir = if self.last_export_dir.trim().is_empty() {
            match next_project_root_dir() {
                Ok(dir) => {
                    self.last_export_dir = dir.to_string_lossy().to_string();
                    dir
                }
                Err(err) => {
                    self.video_status = format!("Kh?ng tao duoc project video: {}", err);
                    return;
                }
            }
        } else {
            let dir = normalize_project_root(Path::new(&self.last_export_dir));
            self.last_export_dir = dir.to_string_lossy().to_string();
            dir
        };
        if let Err(err) = ensure_project_structure(&output_dir)
            .and_then(|_| write_project_text_files(&output_dir, &self.book_input, &self.book_output))
        {
            self.video_status = format!("Kh?ng chuan bi duoc project video: {}", err);
            return;
        }
        let background_path = (!self.video_background_path.trim().is_empty())
            .then_some(PathBuf::from(self.video_background_path.trim()));
        let font_name = self.video_font_name.trim().to_string();
        let text_color = self.video_text_color.trim().to_string();
        let card_opacity = self.video_card_opacity;
        let font_size = self.video_font_size.clamp(20, 96);
        let corner_tag = self.video_corner_tag.trim().to_string();
        let tag_font_size = self.video_tag_font_size.clamp(36, 120);
        let tag_background_enabled = self.video_tag_background_enabled;
        let tag_position = self.video_tag_position;
        let resolution = self.video_resolution;
        let frame_rate = self.video_frame_rate;
        let pause_ms = self.audiobook_pause_ms;
        let enable_word_highlight = self.video_word_highlight_enabled;
        let subtitle_lead_seconds = self.video_subtitle_lead_seconds as f64;
        let api_key = self.api_key.trim().to_string();
        let cached_timed_lines = self.video_timed_lines.clone();
        let cached_word_segments = self.video_word_segments.clone();
        let cached_srt_path = self.video_srt_path.clone();
        let cached_audio_path = self.video_preview_audio_path.clone();

        self.is_exporting_video = true;
        self.video_cancel_flag = Arc::new(AtomicBool::new(false));
        self.video_status = "Preparing video export...".to_string();
        self.video_progress_fraction = 0.0;
        self.video_progress_label = "Preparing video...".to_string();
        let (tx, rx) = mpsc::channel();
        self.video_rx = Some(rx);
        let progress_tx = tx.clone();
        let cancel_flag = self.video_cancel_flag.clone();

        thread::spawn(move || {
            let _ = tx.send(VideoEvent::Status("Building subtitle timeline from speech cache...".to_string()));
            let result = export_video_from_cache(
                &lines,
                &speeches,
                &audiobook_path,
                &output_dir,
                background_path.as_deref(),
                &font_name,
                &text_color,
                card_opacity,
                font_size,
                &corner_tag,
                tag_font_size,
                tag_background_enabled,
                tag_position,
                resolution,
                frame_rate,
                pause_ms,
                &api_key,
                enable_word_highlight,
                &cached_timed_lines,
                &cached_word_segments,
                &cached_srt_path,
                &cached_audio_path,
                subtitle_lead_seconds,
                &cancel_flag,
                progress_tx,
            );

            match result {
                Ok(path) => {
                    let _ = tx.send(VideoEvent::Done(path));
                }
                Err(err) => {
                    if cancel_flag.load(Ordering::SeqCst) {
                        let _ = tx.send(VideoEvent::Cancelled("Video export cancelled.".to_string()));
                    } else {
                        let _ = tx.send(VideoEvent::Error(err.to_string()));
                    }
                }
            }
        });
    }

    fn start_video_preview_export(&mut self) {
        if !self.can_export_video() {
            self.video_status = "Speech cache is incomplete. Preview export is unavailable.".to_string();
            return;
        }

        let output_dir = if !self.last_export_dir.trim().is_empty() {
            let dir = normalize_project_root(Path::new(&self.last_export_dir));
            self.last_export_dir = dir.to_string_lossy().to_string();
            dir
        } else {
            match next_project_root_dir() {
                Ok(dir) => {
                    self.last_export_dir = dir.to_string_lossy().to_string();
                    dir
                }
                Err(err) => {
                    self.video_status = format!("Kh?ng tao duoc project de xuat preview: {}", err);
                    return;
                }
            }
        };
        if let Err(err) = ensure_project_structure(&output_dir)
            .and_then(|_| write_project_text_files(&output_dir, &self.book_input, &self.book_output))
        {
            self.video_status = format!("Kh?ng chuan bi duoc project de xuat preview: {}", err);
            return;
        }
        let audiobook_path = match self.resolve_latest_project_audiobook(&output_dir) {
            Ok(path) => path,
            Err(err) => {
                self.video_status = format!("Could not prepare the latest audiobook: {}", err);
                return;
            }
        };
        if let Err(err) = self.video_srt_matches_audio(&audiobook_path) {
            self.video_status = err.to_string();
            return;
        }

        let start_seconds = self.video_preview_seek_seconds.max(0.0);
        let duration_seconds = self.video_preview_clip_duration_seconds.clamp(3, 90) as f64;
        let font_name = self.video_font_name.trim().to_string();
        let text_color = self.video_text_color.trim().to_string();
        let card_opacity = self.video_card_opacity;
        let font_size = self.video_font_size.clamp(20, 96);
        let corner_tag = self.video_corner_tag.trim().to_string();
        let tag_font_size = self.video_tag_font_size.clamp(36, 120);
        let tag_background_enabled = self.video_tag_background_enabled;
        let tag_position = self.video_tag_position;
        let resolution = self.video_resolution;
        let frame_rate = self.video_frame_rate;
        let background_path = trimmed_path(&self.video_background_path);
        let timed_lines = self.video_timed_lines.clone();
        let word_segments = self.video_word_segments.clone();

        self.is_exporting_video = true;
        self.video_cancel_flag = Arc::new(AtomicBool::new(false));
        self.video_status = "Exporting preview video...".to_string();
        self.video_progress_fraction = 0.0;
        self.video_progress_label = "Preparing preview clip...".to_string();
        let (tx, rx) = mpsc::channel();
        self.video_rx = Some(rx);
        let cancel_flag = self.video_cancel_flag.clone();

        thread::spawn(move || {
            let _ = tx.send(VideoEvent::Status(format!(
                "Exporting preview from {:.1}s for {} seconds...",
                start_seconds, duration_seconds as u32
            )));
            let result = export_video_preview_segment(
                &timed_lines,
                &audiobook_path,
                &output_dir,
                background_path.as_deref(),
                &font_name,
                &text_color,
                card_opacity,
                font_size,
                &corner_tag,
                tag_font_size,
                tag_background_enabled,
                tag_position,
                resolution,
                frame_rate,
                &word_segments,
                start_seconds,
                duration_seconds,
                &cancel_flag,
                tx.clone(),
            );
            match result {
                Ok(path) => {
                    let _ = tx.send(VideoEvent::Done(path));
                }
                Err(err) => {
                    if cancel_flag.load(Ordering::SeqCst) {
                        let _ = tx.send(VideoEvent::Cancelled("Preview export cancelled.".to_string()));
                    } else {
                        let _ = tx.send(VideoEvent::Error(err.to_string()));
                    }
                }
            }
        });
    }

    fn start_generate_video_srt(&mut self) {
        if !self.can_export_video() {
            self.video_status = "Speech cache is incomplete. Cannot create SRT yet.".to_string();
            return;
        }
        let current_project_root = if self.last_export_dir.trim().is_empty() {
            match next_project_root_dir() {
                Ok(dir) => dir,
                Err(err) => {
                    self.video_status = format!("Could not resolve the current project: {}", err);
                    return;
                }
            }
        } else {
            normalize_project_root(Path::new(&self.last_export_dir))
        };
        let audiobook_path = match self.resolve_latest_project_audiobook(&current_project_root) {
            Ok(path) => path,
            Err(err) => {
                self.video_status = format!("Could not prepare the latest audiobook: {}", err);
                return;
            }
        };
        let api_key = self.api_key.trim().to_string();
        if api_key.is_empty() {
            self.video_status = "Missing Gemini API key for SRT creation.".to_string();
            return;
        }

        let lines = parse_book_lines(&self.book_output);
        let speeches = self.exported_speeches.clone();
        let output_dir = if self.last_export_dir.trim().is_empty() {
            match next_project_root_dir() {
                Ok(dir) => {
                    self.last_export_dir = dir.to_string_lossy().to_string();
                    dir
                }
                Err(err) => {
                    self.video_status = format!("Could not create a project folder for SRT: {}", err);
                    return;
                }
            }
        } else {
            let dir = normalize_project_root(Path::new(&self.last_export_dir));
            self.last_export_dir = dir.to_string_lossy().to_string();
            dir
        };
        if let Err(err) = ensure_project_structure(&output_dir)
            .and_then(|_| write_project_text_files(&output_dir, &self.book_input, &self.book_output))
        {
            self.video_status = format!("Could not prepare the project for SRT creation: {}", err);
            return;
        }
        let pause_ms = self.audiobook_pause_ms;
        let enable_word_highlight = self.video_word_highlight_enabled;
        let subtitle_lead_seconds = self.video_subtitle_lead_seconds as f64;
        let job_id = self.next_srt_job_id;
        self.next_srt_job_id += 1;
        let project_root_string = output_dir.to_string_lossy().to_string();
        let audio_path_string = audiobook_path.to_string_lossy().to_string();
        self.srt_jobs.push(SrtBackgroundJob {
            id: job_id,
            project_root: project_root_string.clone(),
            audio_path: audio_path_string.clone(),
            status: "Queued SRT generation".to_string(),
            progress_fraction: 0.0,
            progress_label: "Queued".to_string(),
            srt_path: String::new(),
            finished: false,
            failed: false,
        });
        self.video_status = format!(
            "Started background SRT job #{} for {}",
            job_id,
            audiobook_path.display()
        );
        let tx = self.srt_job_tx.clone();
        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.srt_job_cancel_flags.insert(job_id, cancel_flag.clone());

        thread::spawn(move || {
            let _ = tx.send(SrtJobEvent::Status {
                job_id,
                message: "Preparing sentence list for SRT...".to_string(),
            });
            let result = build_video_srt_with_gemini_background(
                job_id,
                &api_key,
                &lines,
                &speeches,
                &audiobook_path,
                &output_dir,
                pause_ms,
                enable_word_highlight,
                subtitle_lead_seconds,
                &cancel_flag,
                tx.clone(),
            );
            match result {
                Ok((path, timed_lines, word_segments)) => {
                    let _ = tx.send(SrtJobEvent::Ready {
                        job_id,
                        project_root: project_root_string,
                        audio_path: audio_path_string,
                        path,
                        timed_lines,
                        word_segments,
                    });
                }
                Err(err) => {
                    if cancel_flag.load(Ordering::SeqCst) {
                        let _ = tx.send(SrtJobEvent::Cancelled {
                            job_id,
                            message: "SRT generation cancelled.".to_string(),
                        });
                    } else {
                        let _ = tx.send(SrtJobEvent::Error {
                            job_id,
                            error: err.to_string(),
                        });
                    }
                }
            }
        });
    }

    fn start_video_preview_playback(&mut self) {
        if self.video_timed_lines.is_empty() {
            self.video_status = "Ch?a c? SRT ?? preview. H?y b?m 'T?o SRT' tr??c.".to_string();
            return;
        }
        let audio_path = if !self.video_preview_audio_path.trim().is_empty() {
            self.video_preview_audio_path.clone()
        } else if !self.video_srt_path.trim().is_empty() {
            find_preview_audio_for_srt(Path::new(&self.video_srt_path))
                .map(|path| path.to_string_lossy().to_string())
                .unwrap_or_default()
        } else {
            self.last_audiobook_path.clone()
        };
        if audio_path.trim().is_empty() || !Path::new(&audio_path).exists() {
            self.video_status = "Kh?ng t?m th?y audiobook ?? preview.".to_string();
            return;
        }
        self.stop_audio();
        match read_wav_samples(Path::new(&audio_path)).and_then(|samples| self.play_samples(samples)) {
            Ok(()) => {
                self.video_preview_started_at = Some(Instant::now());
                self.video_preview_audio_path = audio_path;
                self.video_status = "?ang preview audio + subtitle.".to_string();
            }
            Err(err) => {
                self.video_status = format!("Kh?ng ph?t ???c preview video: {}", err);
            }
        }
    }

    fn is_export_excluded(&self, speaker: &str) -> bool {
        self.export_excluded_characters
            .iter()
            .any(|item| item.eq_ignore_ascii_case(speaker))
    }

    fn effective_speaker_export_mode(&self, speaker: &str) -> SpeakerExportMode {
        if let Some(setting) = self
            .export_speaker_settings
            .iter()
            .find(|item| item.speaker.eq_ignore_ascii_case(speaker))
        {
            return setting.mode;
        }
        if self.is_export_excluded(speaker) {
            SpeakerExportMode::Exclude
        } else if self.export_render_mode == ExportRenderMode::ByCharacter {
            SpeakerExportMode::Grouped
        } else {
            SpeakerExportMode::PerLine
        }
    }

    fn effective_voice_character_for_speaker(&self, speaker: &str) -> Option<String> {
        let mapped = self
            .export_speaker_settings
            .iter()
            .find(|item| item.speaker.eq_ignore_ascii_case(speaker))
            .map(|item| item.voice_character.trim().to_string())
            .filter(|item| !item.is_empty());

        if mapped.is_some() {
            mapped
        } else if self
            .characters
            .iter()
            .any(|item| item.name.eq_ignore_ascii_case(speaker))
        {
            Some(speaker.to_string())
        } else {
            None
        }
    }

    fn set_speaker_export_mode(&mut self, speaker: &str, mode: SpeakerExportMode) {
        if let Some(existing) = self
            .export_speaker_settings
            .iter_mut()
            .find(|item| item.speaker.eq_ignore_ascii_case(speaker))
        {
            existing.mode = mode;
        } else {
            self.export_speaker_settings.push(SpeakerExportSetting {
                speaker: speaker.to_string(),
                mode,
                voice_character: String::new(),
            });
        }

        if mode == SpeakerExportMode::Exclude {
            if self
                .export_excluded_characters
                .iter()
                .all(|item| !item.eq_ignore_ascii_case(speaker))
            {
                self.export_excluded_characters.push(speaker.to_string());
            }
        } else {
            self.export_excluded_characters
                .retain(|item| !item.eq_ignore_ascii_case(speaker));
        }
    }

    fn set_speaker_voice_character(&mut self, speaker: &str, voice_character: &str) {
        if let Some(existing) = self
            .export_speaker_settings
            .iter_mut()
            .find(|item| item.speaker.eq_ignore_ascii_case(speaker))
        {
            existing.voice_character = voice_character.to_string();
        } else {
            self.export_speaker_settings.push(SpeakerExportSetting {
                speaker: speaker.to_string(),
                mode: self.effective_speaker_export_mode(speaker),
                voice_character: voice_character.to_string(),
            });
        }
    }

    fn collect_reusable_excluded_speeches(
        &self,
        lines: &[BookLine],
    ) -> Result<Vec<(ExportedSpeech, Vec<i16>)>> {
        let mut retained = Vec::new();
        for (index, line) in lines.iter().enumerate() {
            if self.effective_speaker_export_mode(&line.speaker) != SpeakerExportMode::Exclude {
                continue;
            }

            let Some(existing) = self
                .exported_speeches
                .iter()
                .find(|item| {
                    item.index == index
                        && item.speaker.eq_ignore_ascii_case(&line.speaker)
                        && item.text.trim() == line.text.trim()
                })
                .cloned()
            else {
                return Err(anyhow!(
                    "Nh?n v?t '{}' ?ang b? lo?i tr? nh?ng d?ng {} ch?a c? cache h?p l?.",
                    line.speaker,
                    index + 1
                ));
            };

            let samples = read_wav_samples(Path::new(&existing.audio_path)).with_context(|| {
                format!(
                    "Kh?ng ??c ???c cache c? cho '{}' ? d?ng {}",
                    line.speaker,
                    index + 1
                )
            })?;
            retained.push((existing, samples));
        }
        Ok(retained)
    }

    fn missing_cached_book_line_indices(&self) -> Vec<usize> {
        let lines = parse_book_lines(&self.book_output);
        let mut missing = Vec::new();
        for index in 0..lines.len() {
            if self.exported_speeches.iter().all(|item| item.index != index) {
                missing.push(index);
            }
        }
        missing
    }

    fn missing_cached_book_lines(&self) -> Vec<(usize, BookLine)> {
        let lines = parse_book_lines(&self.book_output);
        self.missing_cached_book_line_indices()
            .into_iter()
            .filter_map(|index| lines.get(index).cloned().map(|line| (index, line)))
            .collect()
    }

    fn missing_renderable_book_line_indices(&self) -> Vec<usize> {
        let lines = parse_book_lines(&self.book_output);
        self.missing_cached_book_line_indices()
            .into_iter()
            .filter(|index| {
                lines.get(*index)
                    .map(|line| self.effective_speaker_export_mode(&line.speaker) != SpeakerExportMode::Exclude)
                    .unwrap_or(false)
            })
            .collect()
    }

    fn missing_renderable_book_lines(&self) -> Vec<(usize, BookLine)> {
        let lines = parse_book_lines(&self.book_output);
        self.missing_renderable_book_line_indices()
            .into_iter()
            .filter_map(|index| lines.get(index).cloned().map(|line| (index, line)))
            .collect()
    }

    fn can_export_full_audio(&self) -> bool {
        !parse_book_lines(&self.book_output).is_empty()
            && self.missing_cached_book_line_indices().is_empty()
    }

    fn rebuild_audiobook_from_cache(&mut self) -> Result<()> {
        let lines = parse_book_lines(&self.book_output);
        if lines.is_empty() {
            return Err(anyhow!("Ch?a c? d?ng tho?i ?? gh?p l?i audiobook."));
        }

        let missing = self.missing_cached_book_line_indices();
        if !missing.is_empty() {
            let missing_labels = missing
                .iter()
                .map(|idx| (idx + 1).to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(anyhow!(
                "Ch?a th? gh?p l?i audiobook. C?n thi?u cache ? d?ng: {}",
                missing_labels
            ));
        }

        let output_dir = if self.last_export_dir.trim().is_empty()
            || self.last_export_dir.trim() == EXPORT_DIR
        {
            let dir = next_project_root_dir()?;
            ensure_project_structure(&dir)?;
            self.last_export_dir = dir.to_string_lossy().to_string();
            dir
        } else {
            let dir = PathBuf::from(self.last_export_dir.clone());
            ensure_project_structure(&dir)?;
            dir
        };
        write_project_text_files(&output_dir, &self.book_input, &self.book_output)?;

        let mut speeches = self.exported_speeches.clone();
        speeches.sort_by_key(|item| item.index);
        let paths = speeches
            .iter()
            .map(|item| PathBuf::from(item.audio_path.clone()))
            .collect::<Vec<_>>();
        let previous_audiobook_path = self.last_audiobook_path.clone();
        let audiobook_path = next_numbered_file_path(&project_audio_dir(&output_dir), "audio", "wav")?;
        merge_wav_files(&paths, &audiobook_path, self.audiobook_pause_ms)?;
        self.last_audiobook_path = audiobook_path.to_string_lossy().to_string();
        self.video_preview_audio_path = self.last_audiobook_path.clone();
        if normalize_separators(Path::new(&previous_audiobook_path))
            != normalize_separators(Path::new(&self.last_audiobook_path))
        {
            clear_directory_files(&project_subtitle_dir(&output_dir))?;
            self.video_srt_path.clear();
            self.video_timed_lines.clear();
            self.video_word_segments.clear();
            self.last_video_export_path.clear();
            self.video_status =
                "Audiobook rebuilt. Old subtitle files were cleared. Create a new SRT."
                    .to_string();
        }
        let _ = self.persist_state_to_disk();
        Ok(())
    }

    fn stop_audio(&mut self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
        if let Some(sink) = &self.sink {
            sink.stop();
        }
        self.stream = None;
        self.sink = None;
        self.is_playing = false;
        self.is_fetching = false;
    }

    fn stop_book_export(&mut self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
        self.export_status = "?ang d?ng xu?t audio book...".to_string();
        self.export_qwen_status = "Qwen ?ang d?ng...".to_string();
        self.export_gemini_status = "Gemini ?ang d?ng...".to_string();
    }

    fn stop_video_job(&mut self) {
        self.video_cancel_flag.store(true, Ordering::SeqCst);
        self.video_status = "Stopping video job...".to_string();
        self.video_progress_label = "Stopping video job...".to_string();
    }

    fn stop_srt_job(&mut self, job_id: u64) {
        if let Some(flag) = self.srt_job_cancel_flags.get(&job_id) {
            flag.store(true, Ordering::SeqCst);
        }
        if let Some(job) = self.srt_jobs.iter_mut().find(|job| job.id == job_id) {
            job.status = "Stopping SRT job...".to_string();
            job.progress_label = "Stopping...".to_string();
        }
        self.video_status = format!("Stopping SRT job #{}...", job_id);
    }

    fn load_project(&mut self, root: &Path) -> Result<()> {
        let root = normalize_project_root(root);
        ensure_project_structure(&root)?;

        self.last_export_dir = root.to_string_lossy().to_string();
        self.project_picker_selected = self.last_export_dir.clone();

        let source_path = project_text_dir(&root).join("text_goc.txt");
        self.book_input = if source_path.exists() {
            fs::read_to_string(&source_path)
                .with_context(|| format!("Kh?ng doc duoc {}", source_path.display()))?
        } else {
            String::new()
        };

        let output_path = project_text_dir(&root).join("text_tach_narrator_thoai.txt");
        self.book_output = if output_path.exists() {
            fs::read_to_string(&output_path)
                .with_context(|| format!("Kh?ng doc duoc {}", output_path.display()))?
        } else {
            String::new()
        };

        self.last_audiobook_path = find_latest_audiobook_in_project(&root)
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_default();

        self.video_srt_path = latest_srt_in_project(&root)
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_default();
        self.video_timed_lines.clear();
        self.video_word_segments.clear();
        if !self.video_srt_path.trim().is_empty() {
            let srt_path = PathBuf::from(self.video_srt_path.clone());
            let _ = self.load_video_srt_from_path(&srt_path);
        }

        self.video_background_path = latest_background_in_project(&root)
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_default();
        self.video_preview_texture = None;
        self.video_preview_texture_path.clear();
        self.last_video_export_path = latest_video_in_project(&root)
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_default();

        let book_lines = parse_book_lines(&self.book_output);
        self.exported_speeches = rebuild_project_speeches(&root, &book_lines);
        self.export_skip_details.clear();
        self.export_skipped_indices.clear();
        self.show_project_picker = false;
        self.status = format!("?? m? project: {}", root.display());
        let _ = self.persist_state_to_disk();
        Ok(())
    }

    fn start_speak_main(&mut self) {
        self.start_speak_with_text(self.text.trim().to_string(), false);
    }

    fn start_speak_preview(&mut self, voice: Option<String>) {
        if let Some(v) = voice {
            self.voice_name = v;
        }

        let preview_text = if !self.preview_text.trim().is_empty() {
            self.preview_text.trim().to_string()
        } else if !self.text.trim().is_empty() {
            self.text.trim().to_string()
        } else {
            DEFAULT_PREVIEW_TEXT.to_string()
        };

        self.start_speak_with_text(preview_text, true);
    }

    fn start_speak_with_text(&mut self, text: String, is_preview: bool) {
        let api_key = self.api_key.trim().to_string();
        let voice_name = self.voice_name.trim().to_string();

        if api_key.is_empty() {
            self.status = "Thi?u Gemini API key.".to_string();
            return;
        }
        if text.trim().is_empty() {
            self.status = "B?n chua nh?p van b?n c?n ??c.".to_string();
            return;
        }
        if voice_name.is_empty() {
            self.status = "Voice name kh?ng ???c r?ng.".to_string();
            return;
        }

        self.stop_audio();

        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.cancel_flag = cancel_flag.clone();
        self.is_fetching = true;
        self.status = if is_preview {
            format!("?ang preview voice {}...", self.voice_name)
        } else {
            "?ang g?i Gemini Live TTS...".to_string()
        };

        let job = TtsJob {
            api_key,
            text,
            style_instruction: self.style_instruction.clone(),
            voice_name,
            speed: self.speed,
        };

        let (tx, rx) = mpsc::channel();
        self.tts_rx = Some(rx);

        thread::spawn(move || {
            let _ = tx.send(TtsEvent::Status("?ang k?t n?i Gemini Live...".to_string()));
            match run_tts_job(job, cancel_flag) {
                Ok(samples) => {
                    let _ = tx.send(TtsEvent::Audio(samples));
                }
                Err(err) => {
                    let _ = tx.send(TtsEvent::Error(err.to_string()));
                }
            }
        });
    }

    fn start_voice_analysis(&mut self) {
        let api_key = self.api_key.trim().to_string();
        let Some(audio_path) = self.dropped_audio_path.clone() else {
            self.analysis_status = "B?n ch?a k?o file audio v?o app.".to_string();
            return;
        };

        if api_key.is_empty() {
            self.analysis_status = "Thi?u Gemini API key ?? ph?n t?ch audio.".to_string();
            return;
        }

        self.is_analyzing = true;
        self.analysis_status = "?ang ph?n t?ch voice sample b?ng Gemini...".to_string();

        let job = AnalysisJob {
            api_key,
            audio_path,
        };

        let (tx, rx) = mpsc::channel();
        self.analysis_rx = Some(rx);

        thread::spawn(move || {
            let _ = tx.send(AnalysisEvent::Status(
                "?ang t?i audio clip l?n prompt ph?n t?ch...".to_string(),
            ));
            match analyze_voice_with_gemini(&job) {
                Ok(result) => {
                    let _ = tx.send(AnalysisEvent::Completed(result));
                }
                Err(err) => {
                    let _ = tx.send(AnalysisEvent::Error(err.to_string()));
                }
            }
        });
    }

    fn start_book_formatting(&mut self) {
        let api_key = self.api_key.trim().to_string();
        let source_text = self.book_input.trim().to_string();

        if api_key.is_empty() {
            self.book_status = "Thi?u Gemini API key d? x? l? book text.".to_string();
            return;
        }
        if source_text.is_empty() {
            self.book_status = "B?n ch?a d?n ?o?n truy?n c?n x? l?.".to_string();
            return;
        }

        self.is_formatting_book = true;
        self.book_status = "?ang nh? Gemini t?ch narrator v? tho?i...".to_string();

        let job = BookFormatJob {
            api_key,
            source_text,
        };

        let (tx, rx) = mpsc::channel();
        self.book_rx = Some(rx);

        thread::spawn(move || {
            let _ = tx.send(BookEvent::Status(
                "?ang g?i book text l?n Gemini...".to_string(),
            ));
            match format_book_text_with_gemini(&job) {
                Ok(result) => {
                    let _ = tx.send(BookEvent::Completed(result));
                }
                Err(err) => {
                    let _ = tx.send(BookEvent::Error(err.to_string()));
                }
            }
        });
    }
    fn start_character_analysis(&mut self) {
        let api_key = self.api_key.trim().to_string();
        let source_text = if !self.book_output.trim().is_empty() {
            self.book_output.trim().to_string()
        } else {
            self.book_input.trim().to_string()
        };

        if api_key.is_empty() {
            self.character_status = "Thi?u Gemini API key ?? ph?n t?ch nh?n v?t.".to_string();
            return;
        }
        if source_text.is_empty() {
            self.character_status = "Ch?a c? book text ?? ph?n t?ch nh?n v?t.".to_string();
            return;
        }

        self.is_analyzing_characters = true;
        self.character_status = "?ang ph?n t?ch nh?n v?t b?ng Gemini...".to_string();

        let job = CharacterAnalysisJob {
            api_key,
            source_text,
        };

        let (tx, rx) = mpsc::channel();
        self.character_rx = Some(rx);

        thread::spawn(move || {
            let _ = tx.send(CharacterEvent::Status(
                "?ang g?i ?o?n truy?n l?n Gemini ?? t?m nh?n v?t...".to_string(),
            ));
            match analyze_characters_with_gemini(&job) {
                Ok(result) => {
                    let _ = tx.send(CharacterEvent::Completed(result));
                }
                Err(err) => {
                    let _ = tx.send(CharacterEvent::Error(err.to_string()));
                }
            }
        });
    }

    fn persisted_state(&self) -> PersistedState {
        PersistedState {
            api_key: self.api_key.trim().to_string(),
            book_input: self.book_input.clone(),
            book_output: self.book_output.clone(),
            characters: self.characters.clone(),
            audiobook_pause_ms: self.audiobook_pause_ms,
            exported_speeches: self.exported_speeches.clone(),
            line_volume_settings: self.line_volume_settings.clone(),
            export_render_mode: self.export_render_mode,
            export_excluded_characters: self.export_excluded_characters.clone(),
            export_speaker_settings: self.export_speaker_settings.clone(),
            auto_merge_after_render: self.auto_merge_after_render,
            video_background_path: self.video_background_path.clone(),
            video_corner_tag: self.video_corner_tag.clone(),
            video_tag_position: self.video_tag_position,
            video_tag_font_size: self.video_tag_font_size,
            video_tag_background_enabled: self.video_tag_background_enabled,
            video_font_name: self.video_font_name.clone(),
            video_text_color: self.video_text_color.clone(),
            video_card_opacity: self.video_card_opacity,
            video_font_size: self.video_font_size,
            video_resolution: self.video_resolution,
            video_frame_rate: self.video_frame_rate,
            last_export_dir: self.last_export_dir.clone(),
            last_audiobook_path: self.last_audiobook_path.clone(),
            video_srt_path: self.video_srt_path.clone(),
            video_preview_audio_path: self.video_preview_audio_path.clone(),
            video_preview_clip_duration_seconds: self.video_preview_clip_duration_seconds,
            video_word_highlight_enabled: self.video_word_highlight_enabled,
            video_subtitle_lead_seconds: self.video_subtitle_lead_seconds,
        }
    }

    fn persist_state_to_disk(&mut self) -> bool {
        if let Err(err) = save_persisted_state(&self.persisted_state()) {
            self.status = format!("Kh?ng l?u ???c app-data: {}", err);
            return false;
        }
        true
    }

    fn load_selected_character(&mut self, index: usize) {
        if let Some(record) = self.characters.get(index).cloned() {
            self.selected_character = Some(index);
            self.character_name_input = record.name;
            self.character_description_input = record.description;
            self.character_ref_text_input = record.ref_text;
            self.character_ref_audio_input = record.ref_audio_path;
            self.character_tts_engine_input = record.tts_engine;
            self.character_gemini_voice_input = record.gemini_voice;
            self.character_gemini_style_prompt_input = record.gemini_style_prompt;
            self.character_gemini_speed_input = record.gemini_speed;
            self.character_status = "?? n?p nh?n v?t t? th? vi?n.".to_string();
        }
    }

    fn add_character(&mut self) {
        self.selected_character = None;
        self.character_name_input.clear();
        self.character_description_input.clear();
        self.character_ref_text_input.clear();
        self.character_ref_audio_input.clear();
        self.character_tts_engine_input = "qwen".to_string();
        self.character_gemini_voice_input = DEFAULT_VOICE.to_string();
        self.character_gemini_style_prompt_input.clear();
        self.character_gemini_speed_input = SpeechSpeed::Normal;
        if let Some(path) = &self.dropped_audio_path {
            self.character_ref_audio_input = path.to_string_lossy().to_string();
        }
        self.character_status = "?ang t?o nh?n v?t m?i.".to_string();
    }

    fn start_character_ref_text_extraction(&mut self) {
        let api_key = self.api_key.trim().to_string();
        let audio_path = self.character_ref_audio_input.trim().to_string();
        if api_key.is_empty() {
            self.qwen_status = "Thi?u Gemini API key ?? t?ch ref text.".to_string();
            return;
        }
        if audio_path.is_empty() {
            self.qwen_status = "Ch?a c? ref audio ?? t?ch ch?.".to_string();
            return;
        }

        self.is_qwen_busy = true;
        self.qwen_status = "?ang d?ng Gemini t?ch ref text...".to_string();
        let current_name = if self.character_name_input.trim().is_empty() {
            None
        } else {
            Some(self.character_name_input.trim().to_string())
        };
        let language = self.qwen_language.clone();
        let path = PathBuf::from(audio_path);
        let (tx, rx) = mpsc::channel();
        self.qwen_rx = Some(rx);

        thread::spawn(move || {
            let _ = tx.send(QwenEvent::Status(
                "?ang g?i ref audio l?n Gemini ?? t?ch ch?...".to_string(),
            ));
            match transcribe_audio_with_gemini(&api_key, &path, Some(language.as_str())) {
                Ok(text) => {
                    let _ = tx.send(QwenEvent::RefTextReady {
                        character_name: current_name,
                        text,
                    });
                }
                Err(err) => {
                    let _ = tx.send(QwenEvent::Error(err.to_string()));
                }
            }
        });
    }

    fn start_character_preview(&mut self) {
        let name = self.character_name_input.trim().to_string();
        if name.is_empty() {
            self.qwen_status = "Ch?a c? t?n nh?n v?t ?? preview.".to_string();
            return;
        }
        let description = self.character_description_input.trim().to_string();
        if description.is_empty() {
            self.qwen_status = "Nh?n v?t ch?a c? m? t? ?? preview.".to_string();
            return;
        }
        if self.character_tts_engine_input == "gemini" {
            self.start_gemini_preview_job(
                Some(name),
                self.character_gemini_voice_input.clone(),
                self.character_gemini_style_prompt_input.trim().to_string(),
                self.character_gemini_speed_input,
                description,
                self.characters
                    .iter()
                    .find(|item| item.name.eq_ignore_ascii_case(self.character_name_input.trim()))
                    .map(|item| item.volume_percent)
                    .unwrap_or(100),
            );
        } else {
            self.start_qwen_preview_job(
                Some(name),
                self.character_ref_audio_input.trim().to_string(),
                self.character_ref_text_input.trim().to_string(),
                description,
                self.characters
                    .iter()
                    .find(|item| item.name.eq_ignore_ascii_case(self.character_name_input.trim()))
                    .map(|item| item.volume_percent)
                    .unwrap_or(100),
            );
        }
    }

    fn start_book_line_preview(&mut self, index: usize) {
        let lines = parse_book_lines(&self.book_output);
        let Some(line) = lines.get(index).cloned() else {
            self.qwen_status = "D?ng tho?i kh?ng c?n h?p l?.".to_string();
            return;
        };
        let Some(character) = self.find_character_by_name(&line.speaker).cloned() else {
            self.qwen_status = format!("Ch?a c? voice ?? l?u cho nh?n v?t '{}'.", line.speaker);
            return;
        };
        self.selected_book_line = Some(index);
        let final_gain_percent =
            ((character.volume_percent as u64 * self.line_volume_percent(index) as u64) / 100)
                .clamp(40, 400) as u32;
        if character.tts_engine == "gemini" {
            self.start_gemini_preview_job(
                Some(character.name),
                character.gemini_voice,
                character.gemini_style_prompt,
                character.gemini_speed,
                line.text,
                final_gain_percent,
            );
        } else {
            self.start_qwen_preview_job(
                Some(character.name),
                character.ref_audio_path,
                character.ref_text,
                line.text,
                final_gain_percent,
            );
        }
    }

    fn start_render_book_line(&mut self, index: usize) {
        let lines = parse_book_lines(&self.book_output);
        let Some(line) = lines.get(index).cloned() else {
            self.qwen_status = "D?ng tho?i kh?ng c?n h?p l?.".to_string();
            self.export_status = self.qwen_status.clone();
            return;
        };
        let Some(character) = self.find_character_by_name(&line.speaker).cloned() else {
            self.qwen_status = format!("Ch?a c? voice ?? l?u cho nh?n v?t '{}'.", line.speaker);
            self.export_status = self.qwen_status.clone();
            return;
        };

        self.selected_book_line = Some(index);
        self.qwen_status = format!("?ang render ri?ng d?ng {}...", index + 1);
        self.export_status = format!("Rendering line {}...", index + 1);
        let final_gain_percent =
            ((character.volume_percent as u64 * self.line_volume_percent(index) as u64) / 100)
                .clamp(40, 400) as u32;
        let output_path = app_data_dir()
            .join("speech-cache")
            .join("latest")
            .join(format!("{:04}.wav", index + 1));

        if character.tts_engine == "gemini" {
            self.active_gemini_line_renders += 1;
            self.export_gemini_status = format!("Rendering line {} with Gemini...", index + 1);
            self.export_qwen_status = "Qwen idle.".to_string();
            let api_key = self.api_key.trim().to_string();
            let voice_name = if character.gemini_voice.trim().is_empty() {
                DEFAULT_VOICE.to_string()
            } else {
                character.gemini_voice.clone()
            };
            let tx = self.line_render_tx.clone();
            thread::spawn(move || {
                let _ = tx.send(LineRenderEvent::Status {
                    engine: ExportEngine::Gemini,
                    index,
                    message: format!("Rendering Gemini for line {}...", index + 1),
                });
                let result = run_tts_job_resilient(&TtsJob {
                    api_key,
                    text: line.text.clone(),
                    style_instruction: character.gemini_style_prompt.clone(),
                    voice_name,
                    speed: character.gemini_speed,
                })
                .and_then(|samples| {
                    if let Some(parent) = output_path.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    let adjusted = apply_volume_percent(&samples, final_gain_percent);
                    write_samples_to_wav(&output_path, &adjusted)?;
                    Ok(ExportedSpeech {
                        index,
                        speaker: line.speaker.clone(),
                        text: line.text.clone(),
                        audio_path: output_path.to_string_lossy().to_string(),
                        applied_gain_percent: final_gain_percent,
                    })
                });

                match result {
                    Ok(speech) => {
                        let _ = tx.send(LineRenderEvent::Rendered {
                            engine: ExportEngine::Gemini,
                            speech,
                            message: format!("Rendered line {} with Gemini.", index + 1),
                        });
                    }
                    Err(err) => {
                        let _ = tx.send(LineRenderEvent::Error {
                            engine: ExportEngine::Gemini,
                            index,
                            error: format!("Render d?ng {} th?t b?i: {}", index + 1, err),
                        });
                    }
                }
            });
        } else {
            self.active_qwen_line_renders += 1;
            self.export_qwen_status = format!("Rendering line {} with Qwen...", index + 1);
            self.export_gemini_status = "Gemini idle.".to_string();
            if !self.qwen_service_ready {
                self.active_qwen_line_renders = self.active_qwen_line_renders.saturating_sub(1);
                self.qwen_status =
                    "Qwen Service chua ch?y. H?y b?m 'Kh?i d?ng Qwen Service'.".to_string();
                self.export_status = self.qwen_status.clone();
                return;
            }
            let language = self.qwen_language.clone();
            let xvector_only = self.qwen_xvector_only;
            let Some(service_url) = primary_qwen_service_url() else {
                self.active_qwen_line_renders = self.active_qwen_line_renders.saturating_sub(1);
                self.qwen_status = "Kh?ng t?m th?y Qwen Service s?n s?ng.".to_string();
                self.export_status = self.qwen_status.clone();
                return;
            };
            let tx = self.line_render_tx.clone();
            thread::spawn(move || {
                let _ = tx.send(LineRenderEvent::Status {
                    engine: ExportEngine::Qwen,
                    index,
                    message: format!("Rendering Qwen for line {}...", index + 1),
                });
                let temp_dir = app_data_dir()
                    .join("tmp")
                    .join(format!("line_render_{}_{}", index + 1, current_timestamp_ms()));
                let result = qwen_service_generate_with_chunking(
                    &service_url,
                    &character.ref_audio_path,
                    &character.ref_text,
                    &line.text,
                    &language,
                    xvector_only,
                    &temp_dir,
                )
                    .and_then(|result| {
                        if let Some(parent) = output_path.parent() {
                            let _ = fs::create_dir_all(parent);
                        }
                        let adjusted = apply_volume_percent(&result.samples, final_gain_percent);
                        write_samples_to_wav(&output_path, &adjusted)?;
                        Ok(ExportedSpeech {
                            index,
                            speaker: line.speaker.clone(),
                            text: line.text.clone(),
                            audio_path: output_path.to_string_lossy().to_string(),
                            applied_gain_percent: final_gain_percent,
                        })
                    });

                match result {
                    Ok(speech) => {
                        let _ = tx.send(LineRenderEvent::Rendered {
                            engine: ExportEngine::Qwen,
                            speech,
                            message: format!("Rendered line {} with Qwen.", index + 1),
                        });
                    }
                    Err(err) => {
                        let _ = tx.send(LineRenderEvent::Error {
                            engine: ExportEngine::Qwen,
                            index,
                            error: format!("Render d?ng {} th?t b?i: {}", index + 1, err),
                        });
                    }
                }
            });
        }
    }

    fn start_render_selected_character_lines(&mut self) {
        self.start_render_selected_character_lines_with_mode(None);
    }

    fn start_render_selected_character_lines_grouped(&mut self) {
        self.start_render_selected_character_lines_with_mode(Some(ExportRenderMode::ByCharacter));
    }

    fn start_render_selected_character_lines_with_mode(
        &mut self,
        mode_override: Option<ExportRenderMode>,
    ) {
        let Some(selected_index) = self.selected_character else {
            self.character_status = "Ch?a ch?n nh?n v?t ?? render l?i tho?i.".to_string();
            return;
        };
        let Some(character) = self.characters.get(selected_index).cloned() else {
            self.character_status = "Nh?n v?t ?? ch?n kh?ng c?n h?p l?.".to_string();
            return;
        };

        let lines = parse_book_lines(&self.book_output);
        let matching_lines: Vec<(usize, BookLine)> = lines
            .into_iter()
            .enumerate()
            .filter(|(_, line)| line.speaker.eq_ignore_ascii_case(&character.name))
            .collect();

        if matching_lines.is_empty() {
            self.character_status = format!(
                "Kh?ng t?m th?y d?ng tho?i n?o c?a '{}' trong ph?n Book -> tho?i.",
                character.name
            );
            return;
        }

        if character.tts_engine == "gemini" && self.api_key.trim().is_empty() {
            self.character_status =
                format!("'{}' d?ng Gemini nh?ng ?ang thi?u API key.", character.name);
            return;
        }
        if character.tts_engine != "gemini" {
            if character.ref_audio_path.trim().is_empty() {
                self.character_status =
                    format!("'{}' d?ng Qwen nh?ng ch?a c? ref audio.", character.name);
                return;
            }
            if !self.qwen_service_ready {
                self.character_status =
                    "Qwen Service chua ch?y. H?y b?m 'Kh?i d?ng Qwen Service'.".to_string();
                return;
            }
        }

        let api_key = self.api_key.trim().to_string();
        let language = self.qwen_language.clone();
        let xvector_only = self.qwen_xvector_only;
        let line_volume_settings = self.line_volume_settings.clone();
        let service_url = if character.tts_engine == "gemini" {
            None
        } else {
            match primary_qwen_service_url() {
                Some(url) => Some(url),
                None => {
                    self.character_status = "Kh?ng t?m th?y Qwen Service s?n s?ng.".to_string();
                    return;
                }
            }
        };

        let speaker_name = character.name.clone();
        let total = matching_lines.len();
        let export_render_mode = mode_override.unwrap_or(self.export_render_mode);
        let (tx, rx) = mpsc::channel();
        self.qwen_rx = Some(rx);
        self.is_qwen_busy = true;
        self.character_status = if export_render_mode == ExportRenderMode::ByCharacter {
            format!(
                "?ang render g?p {} d?ng c?a '{}' theo mode {}.",
                total,
                speaker_name,
                export_render_mode.as_str()
            )
        } else {
            format!(
                "?ang render l?i {} d?ng c?a '{}' theo mode {}.",
                total,
                speaker_name,
                export_render_mode.as_str()
            )
        };
        self.qwen_status = self.character_status.clone();
        if character.tts_engine == "gemini" {
            self.export_gemini_status = self.character_status.clone();
            self.export_qwen_status = "Qwen ch?a xu?t g?.".to_string();
        } else {
            self.export_qwen_status = self.character_status.clone();
            self.export_gemini_status = "Gemini ch?a xu?t g?.".to_string();
        }
        self.export_skip_details.clear();

        thread::spawn(move || {
            let mut rendered = 0usize;
            let mut skipped = 0usize;
            let tasks: Vec<ExportTask> = matching_lines
                .iter()
                .map(|(line_index, line)| {
                    let line_gain_percent = line_volume_settings
                        .iter()
                        .find(|item| item.index == *line_index)
                        .map(|item| item.gain_percent)
                        .unwrap_or(100);
                    let final_gain_percent =
                        ((character.volume_percent as u64 * line_gain_percent as u64) / 100)
                            .clamp(40, 400) as u32;
                    ExportTask {
                        index: *line_index,
                        line: line.clone(),
                        character: character.clone(),
                        volume_percent: final_gain_percent,
                        output_path: app_data_dir()
                            .join("speech-cache")
                            .join("latest")
                            .join(format!("{:04}.wav", line_index + 1)),
                        temp_dir: app_data_dir().join("tmp").join(format!(
                            "character_render_{}_{}_{}",
                            slugify_name(&speaker_name),
                            line_index + 1,
                            current_timestamp_ms()
                        )),
                        engine: if character.tts_engine == "gemini" {
                            ExportEngine::Gemini
                        } else {
                            ExportEngine::Qwen
                        },
                    }
                })
                .collect();

            let render_batches: Vec<Vec<ExportTask>> = if export_render_mode == ExportRenderMode::ByCharacter {
                let max_chars = if character.tts_engine == "gemini" {
                    GROUPED_GEMINI_MAX_CHARS
                } else {
                    GROUPED_QWEN_MAX_CHARS
                };
                split_grouped_tasks_into_batches(&tasks, max_chars)
            } else {
                tasks.into_iter().map(|task| vec![task]).collect()
            };
            let batch_total = render_batches.len();

            for (batch_index, batch_tasks) in render_batches.into_iter().enumerate() {
                let _ = tx.send(QwenEvent::Status(format!(
                    "?ang render l?i '{}' batch {}/{}...",
                    speaker_name,
                    batch_index + 1,
                    batch_total
                )));

                let batch_failed = if export_render_mode == ExportRenderMode::ByCharacter && batch_tasks.len() > 1 {
                    let joined_text =
                        build_grouped_text(&batch_tasks.iter().map(|task| task.line.clone()).collect::<Vec<_>>());
                    let grouped_result = if character.tts_engine == "gemini" {
                        run_tts_job_resilient(&TtsJob {
                            api_key: api_key.clone(),
                            text: joined_text,
                            style_instruction: format!(
                                "{} {}",
                                character.gemini_style_prompt,
                                "Add a clear pause between each separate line."
                            )
                            .trim()
                            .to_string(),
                            voice_name: if character.gemini_voice.trim().is_empty() {
                                DEFAULT_VOICE.to_string()
                            } else {
                                character.gemini_voice.clone()
                            },
                            speed: character.gemini_speed,
                        })
                        .map_err(|err| anyhow!("Render g?p theo nh?n v?t th?t b?i: {}", err))
                    } else {
                        qwen_service_generate_with_chunking(
                            service_url.as_deref().unwrap_or_default(),
                            &character.ref_audio_path,
                            &character.ref_text,
                            &joined_text,
                            &language,
                            xvector_only,
                            &batch_tasks[0].temp_dir,
                        )
                        .map(|result| result.samples)
                        .map_err(|err| anyhow!("Render g?p theo nh?n v?t th?t b?i: {}", err))
                    };

                    match grouped_result {
                        Ok(samples) => {
                            let text_list = batch_tasks
                                .iter()
                                .map(|task| task.line.text.clone())
                                .collect::<Vec<_>>();
                            let segments = split_samples_for_grouped_render(&samples, &text_list);
                            for (task, segment) in batch_tasks.iter().zip(segments.into_iter()) {
                                let adjusted = apply_volume_percent(&segment, task.volume_percent);
                                if let Some(parent) = task.output_path.parent() {
                                    let _ = fs::create_dir_all(parent);
                                }
                                match write_samples_to_wav(&task.output_path, &adjusted) {
                                    Ok(()) => {
                                        rendered += 1;
                                        let _ = tx.send(QwenEvent::SpeechReady {
                                            speech: ExportedSpeech {
                                                index: task.index,
                                                speaker: task.line.speaker.clone(),
                                                text: task.line.text.clone(),
                                                audio_path: task.output_path.to_string_lossy().to_string(),
                                                applied_gain_percent: task.volume_percent,
                                            },
                                        });
                                    }
                                    Err(err) => {
                                        skipped += 1;
                                        let _ = tx.send(QwenEvent::LineSkipped {
                                            index: task.index,
                                            speaker: task.line.speaker.clone(),
                                            reason: err.to_string(),
                                        });
                                    }
                                }
                            }
                            false
                        }
                        Err(_err) => {
                            let _ = tx.send(QwenEvent::Status(format!(
                                "{} batch {}/{} l?i, ?ang fallback t?ng d?ng...",
                                speaker_name,
                                batch_index + 1,
                                batch_total
                            )));
                            true
                        }
                    }
                } else {
                    true
                };

                if !batch_failed {
                    continue;
                }

                for task in batch_tasks {
                    let _ = tx.send(QwenEvent::Status(format!(
                        "?ang render l?i d?ng {} c?a '{}'...",
                        task.index + 1,
                        speaker_name
                    )));

                    let result: Result<ExportedSpeech> = if character.tts_engine == "gemini" {
                        run_tts_job_resilient(&TtsJob {
                            api_key: api_key.clone(),
                            text: task.line.text.clone(),
                            style_instruction: character.gemini_style_prompt.clone(),
                            voice_name: if character.gemini_voice.trim().is_empty() {
                                DEFAULT_VOICE.to_string()
                            } else {
                                character.gemini_voice.clone()
                            },
                            speed: character.gemini_speed,
                        })
                        .and_then(|samples| {
                            if let Some(parent) = task.output_path.parent() {
                                let _ = fs::create_dir_all(parent);
                            }
                            let adjusted = apply_volume_percent(&samples, task.volume_percent);
                            write_samples_to_wav(&task.output_path, &adjusted)?;
                            Ok(ExportedSpeech {
                                index: task.index,
                                speaker: task.line.speaker.clone(),
                                text: task.line.text.clone(),
                                audio_path: task.output_path.to_string_lossy().to_string(),
                                applied_gain_percent: task.volume_percent,
                            })
                        })
                    } else {
                        qwen_service_generate_with_chunking(
                            service_url.as_deref().unwrap_or_default(),
                            &character.ref_audio_path,
                            &character.ref_text,
                            &task.line.text,
                            &language,
                            xvector_only,
                            &task.temp_dir,
                        )
                        .and_then(|result| {
                            if let Some(parent) = task.output_path.parent() {
                                let _ = fs::create_dir_all(parent);
                            }
                            let adjusted = apply_volume_percent(&result.samples, task.volume_percent);
                            write_samples_to_wav(&task.output_path, &adjusted)?;
                            Ok(ExportedSpeech {
                                index: task.index,
                                speaker: task.line.speaker.clone(),
                                text: task.line.text.clone(),
                                audio_path: task.output_path.to_string_lossy().to_string(),
                                applied_gain_percent: task.volume_percent,
                            })
                        })
                    };

                    match result {
                        Ok(speech) => {
                            rendered += 1;
                            let _ = tx.send(QwenEvent::SpeechReady { speech });
                        }
                        Err(err) => {
                            skipped += 1;
                            let _ = tx.send(QwenEvent::LineSkipped {
                                index: task.index,
                                speaker: task.line.speaker.clone(),
                                reason: err.to_string(),
                            });
                        }
                    }
                }
            }

            let _ = tx.send(QwenEvent::CharacterBatchDone {
                speaker: speaker_name,
                rendered,
                skipped,
                engine: if character.tts_engine == "gemini" {
                    ExportEngine::Gemini
                } else {
                    ExportEngine::Qwen
                },
            });
        });
    }

    fn play_cached_speech(&mut self, index: usize) {
        let Some(speech) = self.exported_speeches.iter().find(|item| item.index == index) else {
            self.export_status = "Ch?a c? speech cache cho d?ng n?y.".to_string();
            return;
        };
        match read_wav_samples(Path::new(&speech.audio_path)).and_then(|samples| self.play_samples(samples)) {
            Ok(()) => {
                self.export_status = format!("?ang ph?t speech cache cho d?ng {}.", index + 1);
            }
            Err(err) => {
                self.export_status = format!("Kh?ng ph?t ???c speech cache: {}", err);
            }
        }
    }

    fn start_gemini_preview_job(
        &mut self,
        character_name: Option<String>,
        voice_name: String,
        style_instruction: String,
        speed: SpeechSpeed,
        target_text: String,
        volume_percent: u32,
    ) {
        let api_key = self.api_key.trim().to_string();
        if api_key.is_empty() {
            self.qwen_status = "Thi?u Gemini API key.".to_string();
            return;
        }
        if target_text.trim().is_empty() {
            self.qwen_status = "Kh?ng c? n?i dung ?? preview.".to_string();
            return;
        }
        self.is_qwen_busy = true;
        self.qwen_status = "?ang t?o preview b?ng Gemini...".to_string();
        let (tx, rx) = mpsc::channel();
        self.qwen_rx = Some(rx);
        let voice_for_job = if voice_name.trim().is_empty() {
            DEFAULT_VOICE.to_string()
        } else {
            voice_name
        };

        thread::spawn(move || {
            let job = TtsJob {
                api_key,
                text: target_text,
                style_instruction,
                voice_name: voice_for_job,
                speed,
            };
            let _ = tx.send(QwenEvent::Status(
                "?ang g?i Gemini Live cho preview...".to_string(),
            ));
            match run_tts_job_resilient(&job) {
                Ok(samples) => {
                    let _ = tx.send(QwenEvent::PreviewReady {
                        character_name,
                        ref_text: None,
                        samples: apply_volume_percent(&samples, volume_percent),
                        message: "?? t?o preview b?ng Gemini.".to_string(),
                    });
                }
                Err(err) => {
                    let _ = tx.send(QwenEvent::Error(err.to_string()));
                }
            }
        });
    }

    fn start_qwen_preview_job(
        &mut self,
        character_name: Option<String>,
        ref_audio_path: String,
        ref_text: String,
        target_text: String,
        volume_percent: u32,
    ) {
        if !self.qwen_service_ready {
            self.qwen_status = "Qwen Service is not running. Click 'Start Qwen Service'.".to_string();
            return;
        }
        if ref_audio_path.trim().is_empty() {
            self.qwen_status = "The character has no ref audio.".to_string();
            return;
        }
        if target_text.trim().is_empty() {
            self.qwen_status = "No text available for preview.".to_string();
            return;
        }

        self.is_qwen_busy = true;
        self.qwen_status = "Creating preview with Qwen...".to_string();
        let language = self.qwen_language.clone();
        let xvector_only = self.qwen_xvector_only;
        let Some(service_url) = primary_qwen_service_url() else {
            self.is_qwen_busy = false;
            self.qwen_status = "No ready Qwen Service was found.".to_string();
            return;
        };
        let output_path = export_dir_for_session().join(format!(
            "preview_{}_{}.wav",
            slugify_name(character_name.as_deref().unwrap_or("speaker")),
            current_timestamp_ms()
        ));
        let (tx, rx) = mpsc::channel();
        self.qwen_rx = Some(rx);

        thread::spawn(move || {
            let _ = tx.send(QwenEvent::Status("Calling Qwen Service...".to_string()));
            let temp_dir = app_data_dir()
                .join("tmp")
                .join(format!("preview_{}", current_timestamp_ms()));
            match qwen_service_generate_with_chunking(
                &service_url,
                &ref_audio_path,
                &ref_text,
                &target_text,
                &language,
                xvector_only,
                &temp_dir,
            ) {
                Ok(result) => {
                    let _ = write_samples_to_wav(&output_path, &result.samples);
                    let _ = tx.send(QwenEvent::PreviewReady {
                        character_name,
                        ref_text: (!result.ref_text.trim().is_empty()).then_some(result.ref_text),
                        samples: apply_volume_percent(&result.samples, volume_percent),
                        message: format!("Preview created: {}", output_path.display()),
                    });
                }
                Err(err) => {
                    let _ = tx.send(QwenEvent::Error(err.to_string()));
                }
            }
        });
    }

    fn start_export_book_audio(&mut self) {
        let lines = parse_book_lines(&self.book_output);
        if lines.is_empty() {
            self.export_status = "No dialogue lines available to export.".to_string();
            return;
        }

        let missing = self.missing_cached_book_lines();
        if !missing.is_empty() {
            let labels = missing
                .iter()
                .map(|(index, line)| format!("{} ({})", index + 1, line.speaker))
                .collect::<Vec<_>>()
                .join(", ");
            self.export_status =
                format!("Cannot export full audio yet. Missing speech cache for: {}", labels);
            self.export_qwen_status = "Render the missing lines first.".to_string();
            self.export_gemini_status = "Render the missing lines first.".to_string();
            self.show_missing_audio_lines = true;
            return;
        }

        match self.rebuild_audiobook_from_cache() {
            Ok(()) => {
                self.export_status = if self.last_audiobook_path.trim().is_empty() {
                    "Audiobook merged from cache.".to_string()
                } else {
                    format!("Audiobook merged: {}", self.last_audiobook_path)
                };
                self.export_qwen_status = "Qwen idle.".to_string();
                self.export_gemini_status = "Gemini idle.".to_string();
            }
            Err(err) => {
                self.export_status = format!("Full audio export failed: {}", err);
            }
        }
    }

    fn start_export_skipped_lines(&mut self) {
        let mut indices = self.export_skipped_indices.clone();
        indices.sort_unstable();
        indices.dedup();
        if indices.is_empty() {
            self.export_status = "No skipped lines are available to render.".to_string();
            return;
        }
        self.start_export_book_audio_internal(Some(indices));
    }

    fn start_render_missing_lines(&mut self) {
        let mut indices = self.missing_renderable_book_line_indices();
        indices.sort_unstable();
        indices.dedup();
        if indices.is_empty() {
            self.export_status = "No renderable unrendered lines found.".to_string();
            return;
        }
        self.export_status = format!(
            "Rendering {} unrendered lines...",
            indices.len()
        );
        self.start_export_book_audio_internal(Some(indices));
    }

    fn start_render_all_lines(&mut self) {
        let lines = parse_book_lines(&self.book_output);
        if lines.is_empty() {
            self.export_status = "No dialogue lines available to render.".to_string();
            return;
        }
        let indices = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| self.effective_speaker_export_mode(&line.speaker) != SpeakerExportMode::Exclude)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if indices.is_empty() {
            self.export_status = "No speakers are enabled for rendering.".to_string();
            return;
        }
        self.export_status = format!("Rendering all {} lines...", indices.len());
        self.start_export_book_audio_internal(Some(indices));
    }

    fn start_export_book_audio_internal(&mut self, indices_filter: Option<Vec<usize>>) {
        let lines = parse_book_lines(&self.book_output);
        if lines.is_empty() {
            self.export_status = "Error: no valid dialogue lines are available for audio export.".to_string();
            self.qwen_status = "No valid dialogue lines are available for audio export.".to_string();
            return;
        }
        if indices_filter.is_none() {
            let missing = self.missing_cached_book_lines();
            if !missing.is_empty() {
                let labels = missing
                    .iter()
                    .map(|(index, line)| format!("{} ({})", index + 1, line.speaker))
                    .collect::<Vec<_>>()
                    .join(", ");
                self.export_status = format!(
                    "Cannot export full audio yet. Missing speech cache for: {}",
                    labels
                );
                self.export_qwen_status = "Render the missing lines first.".to_string();
                self.export_gemini_status = "Render the missing lines first.".to_string();
                self.show_missing_audio_lines = true;
                return;
            }
        }
        if let Some(indices) = &indices_filter {
            if indices.is_empty() {
                self.export_status = "No lines were selected for rendering.".to_string();
                return;
            }
        }

        self.export_status = "Checking speaker mappings and voice configuration...".to_string();
        self.export_qwen_status = "Qwen is idle.".to_string();
        self.export_gemini_status = "Gemini is idle.".to_string();
        let lines_for_validation = if let Some(indices) = &indices_filter {
            lines.iter()
                .enumerate()
                .filter(|(index, _)| indices.contains(index))
                .map(|(_, line)| line.clone())
                .collect::<Vec<_>>()
        } else {
            lines.clone()
        };
        let issues = self.collect_export_issues(&lines_for_validation);
        if !issues.is_empty() {
            self.export_status = format!(
                "Cannot render. Missing or invalid configuration: {}",
                issues.join(" | ")
            );
            self.qwen_status = format!(
                "Ch?a th? export. Thi?u ho?c l?i c?u h?nh ?: {}",
                issues.join(" | ")
            );
            return;
        }

        let mut characters = self.characters.clone();
        let api_key = self.api_key.trim().to_string();
        let language = self.qwen_language.clone();
        let xvector_only = self.qwen_xvector_only;
        let line_volume_settings = self.line_volume_settings.clone();
        let audiobook_pause_ms = self.audiobook_pause_ms;
        let render_mode = self.export_render_mode;
        let excluded_characters = self.export_excluded_characters.clone();
        let export_speaker_settings = self.export_speaker_settings.clone();
        let excluded_line_count = lines
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                indices_filter
                    .as_ref()
                    .map(|indices| indices.contains(index))
                    .unwrap_or(true)
            })
            .map(|(_, line)| line)
            .filter(|line| {
                export_speaker_settings
                    .iter()
                    .find(|item| item.speaker.eq_ignore_ascii_case(&line.speaker))
                    .map(|item| item.mode == SpeakerExportMode::Exclude)
                    .unwrap_or_else(|| {
                        excluded_characters
                            .iter()
                            .any(|item| item.eq_ignore_ascii_case(&line.speaker))
                    })
            })
            .count();
        let retained_speeches = if indices_filter.is_none() {
            match self.collect_reusable_excluded_speeches(&lines_for_validation) {
                Ok(items) => items,
                Err(err) => {
                    self.export_status = format!("Kh?ng th? xu?t v?i danh s?ch lo?i tr?: {}", err);
                    self.qwen_status = self.export_status.clone();
                    return;
                }
            }
        } else {
            Vec::new()
        };
        let output_dir = if self.last_export_dir.trim().is_empty() {
            match next_project_root_dir() {
                Ok(dir) => {
                    self.last_export_dir = dir.to_string_lossy().to_string();
                    dir
                }
                Err(err) => {
                    self.export_status = format!("Kh?ng tao duoc project moi: {}", err);
                    self.qwen_status = self.export_status.clone();
                    return;
                }
            }
        } else {
            normalize_project_root(Path::new(&self.last_export_dir))
        };
        if let Err(err) = ensure_project_structure(&output_dir)
            .and_then(|_| write_project_text_files(&output_dir, &self.book_input, &self.book_output))
        {
            self.export_status = format!("Kh?ng chuan bi duoc project moi: {}", err);
            self.qwen_status = self.export_status.clone();
            return;
        }
        let (tx, rx) = mpsc::channel();
        self.qwen_rx = Some(rx);
        self.is_qwen_busy = true;
        self.is_exporting_book = true;
        let existing_speeches_snapshot = self.exported_speeches.clone();
        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.cancel_flag = cancel_flag.clone();
        self.export_status = if indices_filter.is_some() {
            format!(
                "?ang xu?t l?i {} d?ng b? b? qua...",
                lines_for_validation.len()
            )
        } else if excluded_line_count > 0 {
            format!(
                "?ang xu?t audio book... t?i d?ng cache c?a {} d?ng ?? lo?i tr?.",
                excluded_line_count
            )
        } else {
            "?ang xu?t audio book...".to_string()
        };
        self.qwen_status = "?ang xu?t audio cho book...".to_string();
        self.export_qwen_status = "Qwen ?ang kh?i t?o worker...".to_string();
        self.export_gemini_status = "Gemini ?ang kh?i t?o job...".to_string();
        if indices_filter.is_none() {
            self.export_skip_details.clear();
            self.export_skipped_indices.clear();
        }
        self.export_progress_done = 0;
        self.export_progress_total = lines_for_validation.len();
        self.export_progress_label =
            format!("{} | 0/{}", render_mode.as_str(), lines_for_validation.len());

        thread::spawn(move || {
            let mut created = 0usize;
            let mut skipped = 0usize;
            let mut updates: Vec<(String, String)> = Vec::new();
            let mut speeches: Vec<ExportedSpeech> = if indices_filter.is_some() {
                existing_speeches_snapshot.clone()
            } else {
                Vec::new()
            };
            let _ = fs::create_dir_all(&output_dir);
            let parts_dir = project_audio_lines_dir(&output_dir);
            let _ = fs::create_dir_all(&parts_dir);
            let cache_dir = app_data_dir().join("speech-cache").join("latest");
            if indices_filter.is_none() {
                let _ = fs::remove_dir_all(&cache_dir);
                let _ = fs::create_dir_all(&cache_dir);
            } else {
                let _ = fs::create_dir_all(&cache_dir);
            }
            for (mut speech, samples) in retained_speeches {
                if cancel_flag.load(Ordering::SeqCst) {
                    break;
                }
                let cache_file = cache_dir.join(format!("{:04}.wav", speech.index + 1));
                if write_samples_to_wav(&cache_file, &samples).is_ok() {
                    speech.audio_path = cache_file.to_string_lossy().to_string();
                }
                if let Some(existing) = speeches.iter_mut().find(|item| item.index == speech.index) {
                    *existing = speech;
                } else {
                    speeches.push(speech);
                }
            }
            let mut grouped_tasks: Vec<GroupedExportTask> = Vec::new();
            let mut gemini_tasks = Vec::new();
            let mut qwen_tasks = Vec::new();
            let effective_mode_for_speaker = |speaker: &str| {
                export_speaker_settings
                    .iter()
                    .find(|item| item.speaker.eq_ignore_ascii_case(speaker))
                    .map(|item| item.mode)
                    .unwrap_or_else(|| {
                        if excluded_characters
                            .iter()
                            .any(|item| item.eq_ignore_ascii_case(speaker))
                        {
                            SpeakerExportMode::Exclude
                        } else if render_mode == ExportRenderMode::ByCharacter {
                            SpeakerExportMode::Grouped
                        } else {
                            SpeakerExportMode::PerLine
                        }
                    })
            };
            let effective_voice_for_speaker = |speaker: &str| {
                let mapped = export_speaker_settings
                    .iter()
                    .find(|item| item.speaker.eq_ignore_ascii_case(speaker))
                    .map(|item| item.voice_character.trim().to_string())
                    .filter(|item| !item.is_empty());
                if mapped.is_some() {
                    mapped
                } else if characters
                    .iter()
                    .any(|item| item.name.eq_ignore_ascii_case(speaker))
                {
                    Some(speaker.to_string())
                } else {
                    None
                }
            };

            for (index, line) in lines.iter().enumerate() {
                if cancel_flag.load(Ordering::SeqCst) {
                    break;
                }
                if indices_filter
                    .as_ref()
                    .map(|indices| !indices.contains(&index))
                    .unwrap_or(false)
                {
                    continue;
                }
                if effective_mode_for_speaker(&line.speaker) == SpeakerExportMode::Exclude {
                    continue;
                }
                let Some(mapped_voice_name) = effective_voice_for_speaker(&line.speaker) else {
                    skipped += 1;
                    let _ = tx.send(QwenEvent::LineSkipped {
                        index,
                        speaker: line.speaker.clone(),
                        reason: "Ch?a ch?n voice ?? xu?t cho speaker n?y.".to_string(),
                    });
                    let _ = tx.send(QwenEvent::Status(format!(
                        "B? qua '{}': chua ch?n voice d? xu?t.",
                        line.speaker
                    )));
                    continue;
                };
                let Some(character_index) = characters
                    .iter()
                    .position(|item| item.name.eq_ignore_ascii_case(&mapped_voice_name))
                else {
                    skipped += 1;
                    let _ = tx.send(QwenEvent::LineSkipped {
                        index,
                        speaker: line.speaker.clone(),
                        reason: format!(
                            "Ch?a c? voice ?? l?u cho '{}'.",
                            mapped_voice_name
                        ),
                    });
                    let _ = tx.send(QwenEvent::Status(format!(
                        "B? qua '{}': ch?a c? voice ?? l?u cho '{}'.",
                        line.speaker, mapped_voice_name
                    )));
                    continue;
                };

                let task = ExportTask {
                    index,
                    line: line.clone(),
                    character: characters[character_index].clone(),
                    volume_percent: (((characters[character_index].volume_percent as u64)
                        * line_volume_settings
                            .iter()
                            .find(|item| item.index == index)
                            .map(|item| item.gain_percent)
                            .unwrap_or(100) as u64)
                        / 100)
                        .clamp(40, 400) as u32,
                    output_path: parts_dir.join(format!("{:04}.wav", index + 1)),
                    temp_dir: parts_dir.join(format!("tmp_{:04}", index + 1)),
                    engine: if characters[character_index].tts_engine == "gemini" {
                        ExportEngine::Gemini
                    } else {
                        ExportEngine::Qwen
                    },
                };

                if effective_mode_for_speaker(&line.speaker) == SpeakerExportMode::Grouped {
                    if let Some(existing) = grouped_tasks.iter_mut().find(|item| {
                        item.engine == task.engine
                            && item.speaker.eq_ignore_ascii_case(&task.line.speaker)
                    }) {
                        existing.tasks.push(task);
                    } else {
                        grouped_tasks.push(GroupedExportTask {
                            speaker: task.line.speaker.clone(),
                            character: task.character.clone(),
                            engine: task.engine,
                            tasks: vec![task],
                        });
                    }
                } else if task.engine == ExportEngine::Gemini {
                    gemini_tasks.push(task);
                } else {
                    qwen_tasks.push(task);
                }
            }

            let grouped_qwen_total = grouped_tasks
                .iter()
                .filter(|item| item.engine == ExportEngine::Qwen)
                .map(|item| item.tasks.len())
                .sum::<usize>();
            let grouped_gemini_total = grouped_tasks
                .iter()
                .filter(|item| item.engine == ExportEngine::Gemini)
                .map(|item| item.tasks.len())
                .sum::<usize>();
            let total_tasks =
                gemini_tasks.len() + qwen_tasks.len() + grouped_qwen_total + grouped_gemini_total;
            let qwen_total = qwen_tasks.len() + grouped_qwen_total;
            let gemini_total = gemini_tasks.len() + grouped_gemini_total;
            let (task_tx, task_rx) = mpsc::channel::<ExportTaskResult>();

            let _ = tx.send(QwenEvent::ExportEngineStatus {
                engine: ExportEngine::Qwen,
                message: if qwen_total == 0 {
                    "Kh?ng c? d?ng n?o d?ng Qwen.".to_string()
                } else if grouped_qwen_total > 0 {
                    format!("?ang ch? {} d?ng Qwen (c? g?p theo speaker)...", qwen_total)
                } else {
                    format!("?ang ch? {} d?ng Qwen...", qwen_total)
                },
            });
            let _ = tx.send(QwenEvent::ExportEngineStatus {
                engine: ExportEngine::Gemini,
                message: if gemini_total == 0 {
                    "Kh?ng c? d?ng n?o d?ng Gemini.".to_string()
                } else if grouped_gemini_total > 0 {
                    format!("?ang ch? {} d?ng Gemini (c? g?p theo speaker)...", gemini_total)
                } else {
                    format!("?ang ch? {} d?ng Gemini...", gemini_total)
                },
            });

            for group in grouped_tasks {
                    if cancel_flag.load(Ordering::SeqCst) {
                        break;
                    }
                    let max_chars = if group.engine == ExportEngine::Gemini {
                        GROUPED_GEMINI_MAX_CHARS
                    } else {
                        GROUPED_QWEN_MAX_CHARS
                    };
                    let batches = split_grouped_tasks_into_batches(&group.tasks, max_chars);
                    let batch_total = batches.len();
                    for (batch_index, batch_tasks) in batches.into_iter().enumerate() {
                        let task_tx = task_tx.clone();
                        let tx_status = tx.clone();
                        let api_key = api_key.clone();
                        let language = language.clone();
                        let service_url = primary_qwen_service_url();
                        let speaker = group.speaker.clone();
                        let character = group.character.clone();
                        let engine = group.engine;
                        let cancel_flag = cancel_flag.clone();
                        thread::spawn(move || {
                            if cancel_flag.load(Ordering::SeqCst) {
                                return;
                            }
                            let engine_name = if engine == ExportEngine::Gemini {
                                "Gemini"
                            } else {
                                "Qwen"
                            };
                            let _ = tx_status.send(QwenEvent::ExportEngineStatus {
                                engine,
                                message: format!(
                                    "{} ?ang render nh?m '{}' batch {}/{} ({} d?ng)...",
                                    engine_name,
                                    speaker,
                                    batch_index + 1,
                                    batch_total,
                                    batch_tasks.len()
                                ),
                            });

                            let group_lines: Vec<BookLine> =
                                batch_tasks.iter().map(|task| task.line.clone()).collect();
                            let joined_text = build_grouped_text(&group_lines);
                            let group_samples = if engine == ExportEngine::Gemini {
                                run_tts_job_resilient(&TtsJob {
                                    api_key: api_key.clone(),
                                    text: joined_text,
                                    style_instruction: format!(
                                        "{} {}",
                                        character.gemini_style_prompt,
                                        "Add a clear pause between each separate line."
                                    )
                                    .trim()
                                    .to_string(),
                                    voice_name: character.gemini_voice.clone(),
                                    speed: character.gemini_speed,
                                })
                            } else {
                                let Some(service_url) = service_url.clone() else {
                                    for task in batch_tasks {
                                        let _ = task_tx.send(ExportTaskResult {
                                            index: task.index,
                                            speaker: task.line.speaker.clone(),
                                            text: task.line.text.clone(),
                                            applied_gain_percent: task.volume_percent,
                                            output_path: None,
                                            ref_text_update: None,
                                            error: Some("Kh?ng tim thay Qwen Service s?n s?ng.".to_string()),
                                            engine: ExportEngine::Qwen,
                                        });
                                    }
                                    return;
                                };
                                qwen_service_generate_with_chunking(
                                    &service_url,
                                    &character.ref_audio_path,
                                    &character.ref_text,
                                    &joined_text,
                                    &language,
                                    xvector_only,
                                    &batch_tasks[0].temp_dir,
                                )
                                .map(|result| result.samples)
                            };

                            match group_samples {
                                Ok(samples) => {
                                    let text_list = batch_tasks
                                        .iter()
                                        .map(|task| task.line.text.clone())
                                        .collect::<Vec<_>>();
                                    let segments = split_samples_for_grouped_render(&samples, &text_list);
                                    for (task, segment) in batch_tasks.into_iter().zip(segments.into_iter()) {
                                        let result = (|| -> Result<ExportTaskResult> {
                                            let adjusted = apply_volume_percent(&segment, task.volume_percent);
                                            write_samples_to_wav(&task.output_path, &adjusted)?;
                                            Ok(ExportTaskResult {
                                                index: task.index,
                                                speaker: task.line.speaker.clone(),
                                                text: task.line.text.clone(),
                                                applied_gain_percent: task.volume_percent,
                                                output_path: Some(task.output_path),
                                                ref_text_update: None,
                                                error: None,
                                                engine: task.engine,
                                            })
                                        })()
                                        .unwrap_or_else(|err| ExportTaskResult {
                                            index: task.index,
                                            speaker: task.line.speaker.clone(),
                                            text: task.line.text.clone(),
                                            applied_gain_percent: task.volume_percent,
                                            output_path: None,
                                            ref_text_update: None,
                                            error: Some(err.to_string()),
                                            engine: task.engine,
                                        });
                                        let _ = task_tx.send(result);
                                    }
                                }
                                Err(err) => {
                                    let grouped_error = err.to_string();
                                    let _ = tx_status.send(QwenEvent::ExportEngineStatus {
                                        engine,
                                        message: format!(
                                            "{} nh?m '{}' batch {}/{} l?i, ?ang fallback t?ng d?ng...",
                                            engine_name,
                                            speaker,
                                            batch_index + 1,
                                            batch_total
                                        ),
                                    });

                                    for task in batch_tasks {
                                        let result = if engine == ExportEngine::Gemini {
                                            run_tts_job_resilient(&TtsJob {
                                                api_key: api_key.clone(),
                                                text: task.line.text.clone(),
                                                style_instruction: character.gemini_style_prompt.clone(),
                                                voice_name: character.gemini_voice.clone(),
                                                speed: character.gemini_speed,
                                            })
                                            .and_then(|samples| {
                                                let adjusted =
                                                    apply_volume_percent(&samples, task.volume_percent);
                                                write_samples_to_wav(&task.output_path, &adjusted)?;
                                                Ok(ExportTaskResult {
                                                    index: task.index,
                                                    speaker: task.line.speaker.clone(),
                                                    text: task.line.text.clone(),
                                                    applied_gain_percent: task.volume_percent,
                                                    output_path: Some(task.output_path.clone()),
                                                    ref_text_update: None,
                                                    error: None,
                                                    engine: task.engine,
                                                })
                                            })
                                            .unwrap_or_else(|line_err| ExportTaskResult {
                                                index: task.index,
                                                speaker: task.line.speaker.clone(),
                                                text: task.line.text.clone(),
                                                applied_gain_percent: task.volume_percent,
                                                output_path: None,
                                                ref_text_update: None,
                                                error: Some(format!(
                                                    "Render g?p theo nh?n v?t th?t b?i: {} | fallback t?ng d?ng th?t b?i: {}",
                                                    grouped_error, line_err
                                                )),
                                                engine: task.engine,
                                            })
                                        } else {
                                            if let Some(service_url) = service_url.clone() {
                                                qwen_service_generate_with_chunking(
                                                    &service_url,
                                                    &character.ref_audio_path,
                                                    &character.ref_text,
                                                    &task.line.text,
                                                    &language,
                                                    xvector_only,
                                                    &task.temp_dir,
                                                )
                                                .and_then(|result| {
                                                    let adjusted = apply_volume_percent(
                                                        &result.samples,
                                                        task.volume_percent,
                                                    );
                                                    write_samples_to_wav(&task.output_path, &adjusted)?;
                                                    Ok(ExportTaskResult {
                                                        index: task.index,
                                                        speaker: task.line.speaker.clone(),
                                                        text: task.line.text.clone(),
                                                        applied_gain_percent: task.volume_percent,
                                                        output_path: Some(task.output_path.clone()),
                                                        ref_text_update: (character.ref_text.trim().is_empty()
                                                            && !result.ref_text.trim().is_empty())
                                                            .then_some((character.name.clone(), result.ref_text)),
                                                        error: None,
                                                        engine: task.engine,
                                                    })
                                                })
                                                .unwrap_or_else(|line_err| ExportTaskResult {
                                                    index: task.index,
                                                    speaker: task.line.speaker.clone(),
                                                    text: task.line.text.clone(),
                                                    applied_gain_percent: task.volume_percent,
                                                    output_path: None,
                                                    ref_text_update: None,
                                                    error: Some(format!(
                                                        "Render g?p theo nh?n v?t th?t b?i: {} | fallback t?ng d?ng th?t b?i: {}",
                                                        grouped_error, line_err
                                                    )),
                                                    engine: task.engine,
                                                })
                                            } else {
                                                ExportTaskResult {
                                                    index: task.index,
                                                    speaker: task.line.speaker.clone(),
                                                    text: task.line.text.clone(),
                                                    applied_gain_percent: task.volume_percent,
                                                    output_path: None,
                                                    ref_text_update: None,
                                                    error: Some(format!(
                                                        "Render g?p theo nh?n v?t th?t b?i: {} | fallback t?ng d?ng th?t b?i: Kh?ng t?m th?y Qwen Service s?n s?ng.",
                                                        grouped_error
                                                    )),
                                                    engine: task.engine,
                                                }
                                            }
                                        };

                                        let _ = task_tx.send(result);
                                    }
                                }
                            }
                        });
                    }
                }

            let qwen_queue = Arc::new(Mutex::new(qwen_tasks));
            for worker_index in 0..QWEN_EXPORT_WORKERS {
                    let queue = qwen_queue.clone();
                    let task_tx = task_tx.clone();
                    let tx_status = tx.clone();
                    let language = language.clone();
                        let service_url = QWEN_SERVICE_URLS[worker_index % QWEN_SERVICE_URLS.len()].to_string();
                        let cancel_flag = cancel_flag.clone();
                        thread::spawn(move || loop {
                        if cancel_flag.load(Ordering::SeqCst) {
                            break;
                        }
                        let task = {
                            let mut queue = queue.lock().expect("qwen queue poisoned");
                            if queue.is_empty() {
                                None
                            } else {
                                Some(queue.remove(0))
                            }
                        };

                        let Some(task) = task else {
                            break;
                        };

                        let _ = tx_status.send(QwenEvent::ExportEngineStatus {
                            engine: ExportEngine::Qwen,
                            message: format!(
                                "Worker {} ?ang xu?t d?ng {} cho {}...",
                                worker_index + 1,
                                task.index + 1,
                                task.line.speaker
                            ),
                        });

                        let speaker = task.line.speaker.clone();
                        let text = task.line.text.clone();
                        let result = qwen_service_generate_with_chunking(
                            &service_url,
                            &task.character.ref_audio_path,
                            &task.character.ref_text,
                            &task.line.text,
                            &language,
                            xvector_only,
                            &task.temp_dir,
                        )
                        .and_then(|result| {
                            let adjusted = apply_volume_percent(&result.samples, task.volume_percent);
                            write_samples_to_wav(&task.output_path, &adjusted)?;
                            Ok(ExportTaskResult {
                                index: task.index,
                                speaker,
                                text,
                                applied_gain_percent: task.volume_percent,
                                output_path: Some(task.output_path),
                                ref_text_update: (task.character.ref_text.trim().is_empty()
                                    && !result.ref_text.trim().is_empty())
                                    .then_some((task.character.name, result.ref_text)),
                                error: None,
                                engine: ExportEngine::Qwen,
                            })
                        })
                        .unwrap_or_else(|err| ExportTaskResult {
                            index: task.index,
                            speaker: task.line.speaker,
                            text: task.line.text,
                            applied_gain_percent: task.volume_percent,
                            output_path: None,
                            ref_text_update: None,
                            error: Some(err.to_string()),
                            engine: ExportEngine::Qwen,
                        });

                        let _ = task_tx.send(result);
                    });
                }

                for task in gemini_tasks {
                    let task_tx = task_tx.clone();
                    let tx_status = tx.clone();
                    let api_key = api_key.clone();
                    let cancel_flag = cancel_flag.clone();
                    thread::spawn(move || {
                        if cancel_flag.load(Ordering::SeqCst) {
                            return;
                        }
                        let _ = tx_status.send(QwenEvent::ExportEngineStatus {
                            engine: ExportEngine::Gemini,
                            message: format!(
                                "?ang xu?t d?ng {} cho {}...",
                                task.index + 1,
                                task.line.speaker
                            ),
                        });

                        let speaker = task.line.speaker.clone();
                        let text = task.line.text.clone();
                        let result = run_tts_job_resilient(&TtsJob {
                            api_key,
                            text: task.line.text.clone(),
                            style_instruction: task.character.gemini_style_prompt.clone(),
                            voice_name: task.character.gemini_voice.clone(),
                            speed: task.character.gemini_speed,
                        })
                        .and_then(|samples| write_samples_to_wav(&task.output_path, &samples))
                        .and_then(|_| {
                            let samples = read_wav_samples(&task.output_path)?;
                            let adjusted = apply_volume_percent(&samples, task.volume_percent);
                            write_samples_to_wav(&task.output_path, &adjusted)?;
                            Ok(())
                        })
                        .map(|_| ExportTaskResult {
                            index: task.index,
                            speaker,
                            text,
                            applied_gain_percent: task.volume_percent,
                            output_path: Some(task.output_path),
                            ref_text_update: None,
                            error: None,
                            engine: ExportEngine::Gemini,
                        })
                        .unwrap_or_else(|err| ExportTaskResult {
                            index: task.index,
                            speaker: task.line.speaker,
                            text: task.line.text,
                            applied_gain_percent: task.volume_percent,
                            output_path: None,
                            ref_text_update: None,
                            error: Some(err.to_string()),
                            engine: ExportEngine::Gemini,
                        });

                        let _ = task_tx.send(result);
                    });
                }

            drop(task_tx);

            let mut qwen_done = 0usize;
            let mut gemini_done = 0usize;
            let _ = tx.send(QwenEvent::ExportProgress {
                qwen_done,
                qwen_total,
                gemini_done,
                gemini_total,
                created,
                skipped,
            });

            for _ in 0..total_tasks {
                if cancel_flag.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(result) = task_rx.recv() else {
                    break;
                };
                match result.engine {
                    ExportEngine::Qwen => qwen_done += 1,
                    ExportEngine::Gemini => gemini_done += 1,
                }
                if let Some(err) = result.error {
                    skipped += 1;
                    let _ = tx.send(QwenEvent::LineSkipped {
                        index: result.index,
                        speaker: result.speaker.clone(),
                        reason: err.clone(),
                    });
                    let _ = tx.send(QwenEvent::Status(format!(
                        "B? qua d?ng {} ({}): {}",
                        result.index + 1,
                        result.speaker,
                        err
                    )));
                    let _ = tx.send(QwenEvent::ExportProgress {
                        qwen_done,
                        qwen_total,
                        gemini_done,
                        gemini_total,
                        created,
                        skipped,
                    });
                    continue;
                }

                if let Some(path) = result.output_path {
                    created += 1;
                    let cache_file = cache_dir.join(format!("{:04}.wav", result.index + 1));
                    let speech_path = if fs::copy(&path, &cache_file).is_ok() {
                        cache_file.to_string_lossy().to_string()
                    } else {
                        path.to_string_lossy().to_string()
                    };
                    let speech = ExportedSpeech {
                        index: result.index,
                        speaker: result.speaker,
                        text: result.text,
                        audio_path: speech_path,
                        applied_gain_percent: result.applied_gain_percent,
                    };
                    speeches.push(speech.clone());
                    let _ = tx.send(QwenEvent::SpeechReady { speech });
                }
                if let Some((name, ref_text)) = result.ref_text_update {
                    if let Some(character_index) = characters
                        .iter()
                        .position(|item| item.name.eq_ignore_ascii_case(&name))
                    {
                        characters[character_index].ref_text = ref_text.clone();
                    }
                    updates.push((name, ref_text));
                }
                let _ = tx.send(QwenEvent::ExportProgress {
                    qwen_done,
                    qwen_total,
                    gemini_done,
                    gemini_total,
                    created,
                    skipped,
                });
            }

            speeches.sort_by_key(|item| item.index);

            let (audiobook_path_opt, merge_error) = if cancel_flag.load(Ordering::SeqCst) {
                (None, Some("?? d?ng xu?t audio book.".to_string()))
            } else {
                let audiobook_path =
                    next_numbered_file_path(&project_audio_dir(&output_dir), "audio", "wav")
                        .unwrap_or_else(|_| project_audio_dir(&output_dir).join("audio_0001.wav"));
                let merge_paths = speeches
                    .iter()
                    .map(|item| PathBuf::from(item.audio_path.clone()))
                    .collect::<Vec<_>>();
                let merge_result = if merge_paths.is_empty() {
                    Err(anyhow!("Kh?ng co file nao de ghep audiobook."))
                } else {
                    merge_wav_files(&merge_paths, &audiobook_path, audiobook_pause_ms)
                };
                (
                    merge_result
                        .as_ref()
                        .ok()
                        .map(|_| audiobook_path.to_string_lossy().to_string()),
                    merge_result.err().map(|err| err.to_string()),
                )
            };
            let _ = tx.send(QwenEvent::ExportDone {
                output_dir: output_dir.to_string_lossy().to_string(),
                created,
                skipped,
                updates,
                audiobook_path: audiobook_path_opt,
                merge_error,
                speeches,
            });
        });
    }

    fn collect_export_issues(&self, lines: &[BookLine]) -> Vec<String> {
        let mut issues = Vec::new();
        let mut seen = std::collections::BTreeSet::new();

        for line in lines {
            let key = line.speaker.to_lowercase();
            if !seen.insert(key) {
                continue;
            }

            if self.effective_speaker_export_mode(&line.speaker) == SpeakerExportMode::Exclude {
                continue;
            }

            let Some(mapped_name) = self.effective_voice_character_for_speaker(&line.speaker) else {
                issues.push(format!("speaker '{}' chua ch?n voice d? xu?t", line.speaker));
                continue;
            };
            let Some(character) = self.find_character_by_name(&mapped_name) else {
                issues.push(format!("thi?u nh?n v?t '{}'", mapped_name));
                continue;
            };

            if character.tts_engine == "gemini" {
                if self.api_key.trim().is_empty() {
                    issues.push(format!("'{}' d?ng Gemini nh?ng thi?u API key", mapped_name));
                }
            } else if character.ref_audio_path.trim().is_empty() {
                issues.push(format!("'{}' d?ng Qwen nh?ng thi?u ref audio", mapped_name));
            }
        }

        issues
    }

    fn save_character_record(&mut self) {
        let name = self.character_name_input.trim().to_string();
        if name.is_empty() {
            self.character_status = "T?n nh?n v?t kh?ng ???c ?? tr?ng.".to_string();
            return;
        }

        let mut stored_audio_path = self.character_ref_audio_input.trim().to_string();
        if !stored_audio_path.is_empty() {
            match copy_character_audio(&name, Path::new(&stored_audio_path)) {
                Ok(path) => {
                    stored_audio_path = path.to_string_lossy().to_string();
                }
                Err(err) => {
                    self.character_status = format!("Kh?ng l?u ???c ref audio: {}", err);
                    return;
                }
            }
        }

        let record = CharacterRecord {
            name: name.clone(),
            description: self.character_description_input.trim().to_string(),
            ref_text: self.character_ref_text_input.trim().to_string(),
            ref_audio_path: stored_audio_path,
            tts_engine: self.character_tts_engine_input.clone(),
            gemini_voice: self.character_gemini_voice_input.clone(),
            gemini_style_prompt: self.character_gemini_style_prompt_input.trim().to_string(),
            gemini_speed: self.character_gemini_speed_input,
            volume_percent: self
                .characters
                .iter()
                .find(|item| item.name.eq_ignore_ascii_case(&name))
                .map(|item| item.volume_percent)
                .unwrap_or(100),
        };

        if let Some(existing) = self
            .characters
            .iter_mut()
            .find(|item| item.name.eq_ignore_ascii_case(&name))
        {
            *existing = record;
        } else {
            self.characters.push(record);
        }

        self.characters
            .sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        self.selected_character = self
            .characters
            .iter()
            .position(|item| item.name.eq_ignore_ascii_case(&name));
        self.persist_state_to_disk();
        self.character_status = "?? l?u nh?n v?t v?o app-data.".to_string();
    }

    fn find_character_by_name(&self, name: &str) -> Option<&CharacterRecord> {
        self.characters
            .iter()
            .find(|item| item.name.eq_ignore_ascii_case(name))
    }

    fn line_volume_percent(&self, index: usize) -> u32 {
        self.line_volume_settings
            .iter()
            .find(|item| item.index == index)
            .map(|item| item.gain_percent)
            .unwrap_or(100)
    }

    fn effective_gain_percent_for_line(&self, index: usize, speaker: &str) -> u32 {
        let character_gain = self
            .find_character_by_name(speaker)
            .map(|item| item.volume_percent)
            .unwrap_or(100);
        (((character_gain as u64) * self.line_volume_percent(index) as u64) / 100)
            .clamp(40, 400) as u32
    }

    fn set_line_volume_percent(&mut self, index: usize, gain_percent: u32) {
        let gain_percent = gain_percent.clamp(40, 220);
        if let Some(item) = self
            .line_volume_settings
            .iter_mut()
            .find(|item| item.index == index)
        {
            item.gain_percent = gain_percent;
        } else {
            self.line_volume_settings.push(LineVolumeSetting { index, gain_percent });
            self.line_volume_settings.sort_by_key(|item| item.index);
        }
        let _ = self.persist_state_to_disk();
    }

    fn apply_gain_to_cached_speech(&mut self, index: usize, new_gain_percent: u32) -> Result<bool> {
        let Some(speech_index) = self
            .exported_speeches
            .iter()
            .position(|item| item.index == index)
        else {
            return Ok(false);
        };

        let speech = self.exported_speeches[speech_index].clone();
        let old_gain = speech.applied_gain_percent.max(1);
        if old_gain == new_gain_percent {
            return Ok(false);
        }

        let path = PathBuf::from(&speech.audio_path);
        if !path.exists() {
            return Ok(false);
        }

        let samples = read_wav_samples(&path)?;
        let ratio = new_gain_percent as f32 / old_gain as f32;
        let adjusted: Vec<i16> = samples
            .iter()
            .map(|sample| {
                let scaled = (*sample as f32 * ratio).round();
                scaled.clamp(i16::MIN as f32, i16::MAX as f32) as i16
            })
            .collect();
        write_samples_to_wav(&path, &adjusted)?;
        self.exported_speeches[speech_index].applied_gain_percent = new_gain_percent;
        let _ = self.persist_state_to_disk();
        Ok(true)
    }

    fn apply_character_volume_to_cached_speeches(&mut self, speaker: &str) -> Result<usize> {
        let targets: Vec<(usize, u32)> = self
            .exported_speeches
            .iter()
            .filter(|item| item.speaker.eq_ignore_ascii_case(speaker))
            .map(|item| {
                (
                    item.index,
                    self.effective_gain_percent_for_line(item.index, &item.speaker),
                )
            })
            .collect();

        let mut changed = 0usize;
        for (index, gain) in targets {
            if self.apply_gain_to_cached_speech(index, gain)? {
                changed += 1;
            }
        }
        if changed > 0 {
            let _ = self.rebuild_audiobook_from_cache();
        }
        Ok(changed)
    }

    fn update_character_ref_text(&mut self, character_name: &str, ref_text: &str) {
        if ref_text.trim().is_empty() {
            return;
        }
        if let Some(item) = self
            .characters
            .iter_mut()
            .find(|item| item.name.eq_ignore_ascii_case(character_name))
        {
            item.ref_text = ref_text.trim().to_string();
        }
        if self.character_name_input.eq_ignore_ascii_case(character_name) {
            self.character_ref_text_input = ref_text.trim().to_string();
        }
        self.persist_state_to_disk();
    }

    fn delete_selected_character(&mut self) {
        let Some(index) = self.selected_character else {
            self.character_status = "Ch?a ch?n nh?n v?t ?? x?a.".to_string();
            return;
        };
        if index < self.characters.len() {
            self.characters.remove(index);
            self.selected_character = None;
            self.character_name_input.clear();
            self.character_description_input.clear();
            self.character_ref_text_input.clear();
            self.character_ref_audio_input.clear();
            self.character_tts_engine_input = "qwen".to_string();
            self.character_gemini_voice_input = DEFAULT_VOICE.to_string();
            self.character_gemini_style_prompt_input.clear();
            self.character_gemini_speed_input = SpeechSpeed::Normal;
            self.persist_state_to_disk();
            self.character_status = "?? x?a nh?n v?t kh?i th? vi?n.".to_string();
        }
    }
    fn play_samples(&mut self, samples: Vec<i16>) -> Result<()> {
        if samples.is_empty() {
            return Err(anyhow!("Kh?ng nh?n ???c audio t? Gemini."));
        }

        self.last_audio_samples = Some(samples.clone());

        let stream = OutputStreamBuilder::open_default_stream()
            .context("Kh?ng mo duoc thiet bi am thanh mac dinh")?;
        let sink = Sink::connect_new(stream.mixer());
        let pcm_f32: Vec<f32> = samples
            .into_iter()
            .map(|s| s as f32 / i16::MAX as f32)
            .collect();

        let source = SamplesBuffer::new(1, SAMPLE_RATE, pcm_f32);
        sink.append(source);
        sink.play();

        self.stream = Some(stream);
        self.sink = Some(sink);
        self.is_playing = true;
        Ok(())
    }

    fn export_last_audio(&mut self) {
        let Some(samples) = &self.last_audio_samples else {
            self.status = "Ch?a c? audio ?? xu?t.".to_string();
            return;
        };

        if let Err(err) = fs::create_dir_all(EXPORT_DIR) {
            self.status = format!("Kh?ng tao duoc thu muc {}: {}", EXPORT_DIR, err);
            return;
        }

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut output_path = PathBuf::from(EXPORT_DIR);
        output_path.push(format!("gemini_tts_{ts}.wav"));

        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: SAMPLE_RATE,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };

        let save_result = (|| -> Result<()> {
            let mut writer = hound::WavWriter::create(&output_path, spec)
                .with_context(|| format!("Kh?ng tao duoc file {}", output_path.display()))?;
            for sample in samples {
                writer
                    .write_sample(*sample)
                    .with_context(|| format!("Loi ghi WAV {}", output_path.display()))?;
            }
            writer
                .finalize()
                .with_context(|| format!("Loi finalize WAV {}", output_path.display()))?;
            Ok(())
        })();

        match save_result {
            Ok(()) => {
                self.status = format!("?? xu?t audio: {}", output_path.display());
            }
            Err(err) => {
                self.status = format!("Xu?t audio th?t b?i: {}", err);
            }
        }
    }

    fn refresh_local_engines(&mut self) {
        if self.last_engine_probe.elapsed() < Duration::from_secs(4) {
            return;
        }

        self.qwen_ready = is_local_port_open(QWEN_PORT);
        let health = get_qwen_service_health_all();
        self.qwen_service_healths = health.clone();
        self.qwen_service_ready = health.iter().any(|(service_ready, _)| *service_ready);
        self.qwen_model_ready = health.iter().any(|(_, model_ready)| *model_ready);
        self.last_engine_probe = Instant::now();
    }

    fn launch_qwen_service(&mut self) {
        let port = QWEN_SERVICE_PORTS[0];
        match launch_batch_in_console_with_args("start_qwen_service.bat", &[&port.to_string()]) {
            Ok(()) => {
                self.qwen_status =
                    "?? m? Qwen Service v?i console. Ch? model n?p xong r?i preview ho?c export."
                        .to_string();
            }
            Err(err) => {
                self.qwen_status = format!("Kh?ng m? ???c Qwen Service {}: {}", port, err);
            }
        }
    }

    fn poll_tts_events(&mut self) {
        let mut events = Vec::new();
        let mut clear_receiver = false;

        if let Some(rx) = &self.tts_rx {
            loop {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.is_fetching = false;
                        clear_receiver = true;
                        break;
                    }
                }
            }
        }

        for event in events {
            match event {
                TtsEvent::Status(msg) => {
                    self.status = msg;
                }
                TtsEvent::Audio(samples) => {
                    self.is_fetching = false;
                    match self.play_samples(samples) {
                        Ok(()) => {
                            self.status = "?ang ph?t audio...".to_string();
                        }
                        Err(err) => {
                            self.status = format!("L?i ph?t audio: {}", err);
                            self.is_playing = false;
                        }
                    }
                    clear_receiver = true;
                }
                TtsEvent::Error(err) => {
                    self.status = format!("L?i: {}", err);
                    self.is_fetching = false;
                    self.is_playing = false;
                    clear_receiver = true;
                }
            }
        }

        if clear_receiver {
            self.tts_rx = None;
        }

        if self.is_playing {
            if let Some(sink) = &self.sink {
                if sink.empty() {
                    self.is_playing = false;
                    self.sink = None;
                    self.stream = None;
                    if !self.cancel_flag.load(Ordering::SeqCst) {
                        self.status = "??c xong.".to_string();
                    }
                }
            } else {
                self.is_playing = false;
            }
        }
    }

    fn poll_analysis_events(&mut self) {
        let mut events = Vec::new();
        let mut clear_receiver = false;

        if let Some(rx) = &self.analysis_rx {
            loop {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.is_analyzing = false;
                        clear_receiver = true;
                        break;
                    }
                }
            }
        }

        for event in events {
            match event {
                AnalysisEvent::Status(msg) => {
                    self.analysis_status = msg;
                }
                AnalysisEvent::Completed(result) => {
                    self.analysis_result = result;
                    self.analysis_status =
                        "Ph?n t?ch xong. B?n c? th? copy ho?c ??a prompt v?o ? TTS.".to_string();
                    self.is_analyzing = false;
                    clear_receiver = true;
                }
                AnalysisEvent::Error(err) => {
                    self.analysis_status = format!("L?i ph?n t?ch: {}", err);
                    self.is_analyzing = false;
                    clear_receiver = true;
                }
            }
        }

        if clear_receiver {
            self.analysis_rx = None;
        }
    }

    fn poll_book_events(&mut self) {
        let mut events = Vec::new();
        let mut clear_receiver = false;

        if let Some(rx) = &self.book_rx {
            loop {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.is_formatting_book = false;
                        clear_receiver = true;
                        break;
                    }
                }
            }
        }

        for event in events {
            match event {
                BookEvent::Status(msg) => {
                    self.book_status = msg;
                }
                BookEvent::Completed(result) => {
                    self.book_output = result;
                    self.book_status = "?? x? l? xong book text.".to_string();
                    self.show_book_source = false;
                    self.show_book_result = true;
                    self.persist_state_to_disk();
                    self.is_formatting_book = false;
                    clear_receiver = true;
                }
                BookEvent::Error(err) => {
                    self.book_status = format!("L?i x? l? book text: {}", err);
                    self.is_formatting_book = false;
                    clear_receiver = true;
                }
            }
        }

        if clear_receiver {
            self.book_rx = None;
        }
    }

    fn poll_character_events(&mut self) {
        let mut events = Vec::new();
        let mut clear_receiver = false;

        if let Some(rx) = &self.character_rx {
            loop {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.is_analyzing_characters = false;
                        clear_receiver = true;
                        break;
                    }
                }
            }
        }

        for event in events {
            match event {
                CharacterEvent::Status(msg) => {
                    self.character_status = msg;
                }
                CharacterEvent::Completed(items) => {
                    for item in items {
                        if self
                            .characters
                            .iter()
                            .all(|existing| !existing.name.eq_ignore_ascii_case(&item.name))
                        {
                            self.characters.push(item);
                        }
                    }
                    self.characters
                        .sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
                    self.persist_state_to_disk();
                    self.character_status = "?? ph?n t?ch v? th?m nh?n v?t v?o th? vi?n.".to_string();
                    self.is_analyzing_characters = false;
                    clear_receiver = true;
                }
                CharacterEvent::Error(err) => {
                    self.character_status = format!("L?i ph?n t?ch nh?n v?t: {}", err);
                    self.is_analyzing_characters = false;
                    clear_receiver = true;
                }
            }
        }

        if clear_receiver {
            self.character_rx = None;
        }
    }

    fn poll_video_events(&mut self) {
        let mut events = Vec::new();
        let mut clear_receiver = false;

        if let Some(rx) = &self.video_rx {
            loop {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.is_exporting_video = false;
                        clear_receiver = true;
                        break;
                    }
                }
            }
        }

        for event in events {
            match event {
                VideoEvent::Status(message) => {
                    self.video_status = message;
                }
                VideoEvent::Progress { fraction, label } => {
                    self.video_progress_fraction = fraction.clamp(0.0, 1.0);
                    self.video_progress_label = label;
                }
                VideoEvent::SrtReady {
                    path,
                    timed_lines,
                    word_segments,
                } => {
                    self.is_exporting_video = false;
                    self.video_timed_lines = timed_lines;
                    self.video_word_segments = word_segments;
                    self.video_srt_path = path.to_string_lossy().to_string();
                    self.video_preview_audio_path = self.last_audiobook_path.clone();
                    self.video_preview_seek_seconds = 0.0;
                    self.video_preview_started_at = None;
                    self.video_progress_fraction = 1.0;
                    self.video_progress_label = "?? t?o xong SRT".to_string();
                    self.video_status = format!("?? t?o SRT: {}", path.display());
                    clear_receiver = true;
                }
                VideoEvent::Done(path) => {
                    self.is_exporting_video = false;
                    self.video_progress_fraction = 1.0;
                    self.video_progress_label = "Ho?n t?t".to_string();
                    self.video_status = format!("?? xu?t video: {}", path.display());
                    self.last_video_export_path = path.to_string_lossy().to_string();
                    self.last_export_dir = normalize_project_root(&path).to_string_lossy().to_string();
                    let _ = self.persist_state_to_disk();
                    clear_receiver = true;
                }
                VideoEvent::Cancelled(message) => {
                    self.is_exporting_video = false;
                    self.video_progress_label = "Cancelled".to_string();
                    self.video_status = message;
                    clear_receiver = true;
                }
                VideoEvent::Error(err) => {
                    self.is_exporting_video = false;
                    self.video_progress_label = "L?i".to_string();
                    self.video_status = format!("L?i xu?t video: {}", err);
                    clear_receiver = true;
                }
            }
        }

        if clear_receiver {
            self.video_rx = None;
        }

        if self.video_preview_started_at.is_some() && !self.is_playing {
            self.video_preview_started_at = None;
        }
    }

    fn poll_srt_job_events(&mut self) {
        loop {
            match self.srt_job_rx.try_recv() {
                Ok(event) => match event {
                    SrtJobEvent::Status { job_id, message } => {
                        if let Some(job) = self.srt_jobs.iter_mut().find(|job| job.id == job_id) {
                            job.status = message.clone();
                            job.progress_label = message.clone();
                        }
                        self.video_status = message;
                    }
                    SrtJobEvent::Progress {
                        job_id,
                        fraction,
                        label,
                    } => {
                        if let Some(job) = self.srt_jobs.iter_mut().find(|job| job.id == job_id) {
                            job.progress_fraction = fraction.clamp(0.0, 1.0);
                            job.progress_label = label.clone();
                            job.status = label.clone();
                        }
                    }
                    SrtJobEvent::Ready {
                        job_id,
                        project_root,
                        audio_path,
                        path,
                        timed_lines,
                        word_segments,
                    } => {
                        if let Some(job) = self.srt_jobs.iter_mut().find(|job| job.id == job_id) {
                            job.finished = true;
                            job.failed = false;
                            job.srt_path = path.to_string_lossy().to_string();
                            job.progress_fraction = 1.0;
                            job.progress_label = "SRT ready".to_string();
                            job.status = format!("SRT ready: {}", path.display());
                        }
                        let current_project = if self.last_export_dir.trim().is_empty() {
                            None
                        } else {
                            Some(normalize_project_root(Path::new(&self.last_export_dir)))
                        };
                        if current_project
                            .as_ref()
                            .is_some_and(|root| normalize_separators(root) == normalize_separators(Path::new(&project_root)))
                        {
                            self.video_timed_lines = timed_lines;
                            self.video_word_segments = word_segments;
                            self.video_srt_path = path.to_string_lossy().to_string();
                            self.video_preview_audio_path = audio_path;
                            self.video_preview_seek_seconds = 0.0;
                            self.video_preview_started_at = None;
                            self.video_preview_line_index = 0;
                            self.video_status =
                                format!("Background SRT ready: {}", path.display());
                            let _ = self.persist_state_to_disk();
                        }
                        self.srt_job_cancel_flags.remove(&job_id);
                    }
                    SrtJobEvent::Error { job_id, error } => {
                        if let Some(job) = self.srt_jobs.iter_mut().find(|job| job.id == job_id) {
                            job.finished = true;
                            job.failed = true;
                            job.progress_label = "Failed".to_string();
                            job.status = error.clone();
                        }
                        self.video_status = format!("Background SRT failed: {}", error);
                        self.srt_job_cancel_flags.remove(&job_id);
                    }
                    SrtJobEvent::Cancelled { job_id, message } => {
                        if let Some(job) = self.srt_jobs.iter_mut().find(|job| job.id == job_id) {
                            job.finished = true;
                            job.failed = true;
                            job.progress_label = "Cancelled".to_string();
                            job.status = message.clone();
                        }
                        self.video_status = message;
                        self.srt_job_cancel_flags.remove(&job_id);
                    }
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }

    fn poll_qwen_events(&mut self) {
        let mut events = Vec::new();
        let mut clear_receiver = false;

        if let Some(rx) = &self.qwen_rx {
            loop {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.is_qwen_busy = false;
                        self.is_exporting_book = false;
                        clear_receiver = true;
                        break;
                    }
                }
            }
        }

        for event in events {
            match event {
                QwenEvent::Status(msg) => {
                    self.qwen_status = msg;
                    if !self.is_exporting_book {
                        self.export_status = self.qwen_status.clone();
                    }
                }
                QwenEvent::ExportEngineStatus { engine, message } => {
                    match engine {
                        ExportEngine::Qwen => self.export_qwen_status = message,
                        ExportEngine::Gemini => self.export_gemini_status = message,
                    }
                }
                QwenEvent::ExportProgress {
                    qwen_done,
                    qwen_total,
                    gemini_done,
                    gemini_total,
                    created,
                    skipped,
                } => {
                    let total_done = qwen_done + gemini_done;
                    let total = qwen_total + gemini_total;
                    self.export_status = format!(
                        "T?ng: {}/{} | T?o: {} | B? qua: {}",
                        total_done, total, created, skipped
                    );
                    self.export_progress_done = total_done;
                    self.export_progress_total = total;
                    self.export_progress_label =
                        format!("{} | {}/{}", self.export_render_mode.as_str(), total_done, total);
                    self.qwen_status = format!(
                        "Qwen {}/{} | Gemini {}/{}",
                        qwen_done, qwen_total, gemini_done, gemini_total
                    );
                    if qwen_total > 0 && qwen_done >= qwen_total {
                        self.export_qwen_status = format!("?? xong {}/{} d?ng.", qwen_done, qwen_total);
                    }
                    if gemini_total > 0 && gemini_done >= gemini_total {
                        self.export_gemini_status =
                            format!("?? xong {}/{} d?ng.", gemini_done, gemini_total);
                    }
                }
                QwenEvent::RefTextReady {
                    character_name,
                    text,
                } => {
                    self.character_ref_text_input = text.clone();
                    if let Some(name) = character_name {
                        self.update_character_ref_text(&name, &text);
                    }
                    self.qwen_status = "?? l?y ref text b?ng Gemini.".to_string();
                    self.is_qwen_busy = false;
                    clear_receiver = true;
                }
                QwenEvent::PreviewReady {
                    character_name,
                    ref_text,
                    samples,
                    message,
                } => {
                    if let Some(name) = character_name {
                        if let Some(text) = ref_text {
                            self.update_character_ref_text(&name, &text);
                        }
                    }
                    self.is_qwen_busy = false;
                    self.is_exporting_book = false;
                    match self.play_samples(samples) {
                        Ok(()) => {
                            self.qwen_status = message;
                        }
                        Err(err) => {
                            self.qwen_status = format!("L?i ph?t audio Qwen: {}", err);
                        }
                    }
                    clear_receiver = true;
                }
                QwenEvent::LineRendered { speech, message } => {
                    let rendered_index = speech.index;
                    let rendered_speaker = speech.speaker.clone();
                    if let Some(existing) = self
                        .exported_speeches
                        .iter_mut()
                        .find(|item| item.index == speech.index)
                    {
                        *existing = speech;
                    } else {
                        self.exported_speeches.push(speech);
                        self.exported_speeches.sort_by_key(|item| item.index);
                    }
                    self.export_skipped_indices
                        .retain(|index| *index != rendered_index);
                    self.export_skip_details
                        .retain(|item| !item.starts_with(&format!("D?ng {} ", rendered_index + 1)));
                    self.persist_state_to_disk();
                    self.qwen_status = message;
                    if self.auto_merge_after_render {
                        match self.rebuild_audiobook_from_cache() {
                            Ok(()) => {
                                self.export_status = format!(
                                    "?? render xong d?ng {} v? gh?p l?i audiobook: {}",
                                    rendered_index + 1,
                                    self.last_audiobook_path
                                );
                            }
                            Err(err) => {
                                self.export_status = format!(
                                    "?? render xong d?ng {}. Ch?a gh?p l?i audiobook: {}",
                                    rendered_index + 1,
                                    err
                                );
                            }
                        }
                    } else {
                        self.export_status = format!(
                            "?? render xong d?ng {}. T? gh?p audio ?ang t?t.",
                            rendered_index + 1
                        );
                    }
                    if self
                        .find_character_by_name(&rendered_speaker)
                        .is_some_and(|character| character.tts_engine == "gemini")
                    {
                        self.export_gemini_status =
                            format!("Line {} rendered successfully.", rendered_index + 1);
                    } else {
                        self.export_qwen_status =
                            format!("Line {} rendered successfully.", rendered_index + 1);
                    }
                    self.is_qwen_busy = false;
                    clear_receiver = true;
                }
                QwenEvent::SpeechReady { speech } => {
                    let rendered_index = speech.index;
                    if let Some(existing) = self
                        .exported_speeches
                        .iter_mut()
                        .find(|item| item.index == speech.index)
                    {
                        *existing = speech;
                    } else {
                        self.exported_speeches.push(speech);
                        self.exported_speeches.sort_by_key(|item| item.index);
                    }
                    self.export_skipped_indices
                        .retain(|index| *index != rendered_index);
                    self.export_skip_details
                        .retain(|item| !item.starts_with(&format!("D?ng {} ", rendered_index + 1)));
                    self.persist_state_to_disk();
                }
                QwenEvent::LineSkipped {
                    index,
                    speaker,
                    reason,
                } => {
                    if !self.export_skipped_indices.contains(&index) {
                        self.export_skipped_indices.push(index);
                    }
                    self.export_skip_details.push(format!(
                        "D?ng {} ({}) b? b? qua: {}",
                        index + 1,
                        speaker,
                        reason
                    ));
                }
                QwenEvent::CharacterBatchDone {
                    speaker,
                    rendered,
                    skipped,
                    engine,
                } => {
                    self.persist_state_to_disk();
                    if self.auto_merge_after_render {
                        match self.rebuild_audiobook_from_cache() {
                            Ok(()) => {
                                self.export_status = format!(
                                    "?? render l?i {} d?ng c?a '{}', b? qua {}. ?? gh?p l?i audiobook: {}",
                                    rendered, speaker, skipped, self.last_audiobook_path
                                );
                            }
                            Err(err) => {
                                self.export_status = format!(
                                    "?? render l?i {} d?ng c?a '{}', b? qua {}. Ch?a gh?p l?i audiobook: {}",
                                    rendered, speaker, skipped, err
                                );
                            }
                        }
                    } else {
                        self.export_status = format!(
                            "?? render l?i {} d?ng c?a '{}', b? qua {}. T? gh?p audio ?ang t?t.",
                            rendered, speaker, skipped
                        );
                    }
                    self.character_status = format!(
                        "?? render l?i tho?i c?a '{}': th?nh c?ng {}, b? qua {}.",
                        speaker, rendered, skipped
                    );
                    self.qwen_status = self.character_status.clone();
                    let engine_message = format!(
                        "?? render l?i tho?i c?a '{}': th?nh c?ng {}, b? qua {}.",
                        speaker, rendered, skipped
                    );
                    match engine {
                        ExportEngine::Qwen => self.export_qwen_status = engine_message,
                        ExportEngine::Gemini => self.export_gemini_status = engine_message,
                    }
                    self.is_qwen_busy = false;
                    clear_receiver = true;
                }
                QwenEvent::ExportDone {
                    output_dir,
                    created,
                    skipped,
                    updates,
                    audiobook_path,
                    merge_error,
                    speeches,
                } => {
                    self.export_skipped_indices.sort_unstable();
                    self.export_skipped_indices.dedup();
                    for (name, ref_text) in updates {
                        self.update_character_ref_text(&name, &ref_text);
                    }
                    let previous_audiobook_path = self.last_audiobook_path.clone();
                    self.last_export_dir = output_dir.clone();
                    self.last_audiobook_path = audiobook_path.unwrap_or_default();
                    self.video_preview_audio_path = self.last_audiobook_path.clone();
                    self.exported_speeches = speeches;
                    let merged_audio_changed = !self.last_audiobook_path.trim().is_empty()
                        && normalize_separators(Path::new(&previous_audiobook_path))
                            != normalize_separators(Path::new(&self.last_audiobook_path));
                    if merge_error.is_none() && merged_audio_changed {
                        let root = normalize_project_root(Path::new(&self.last_export_dir));
                        if let Err(err) = clear_directory_files(&project_subtitle_dir(&root)) {
                            self.video_status = format!(
                                "Audiobook merged, but could not clear subtitles: {}",
                                err
                            );
                        } else {
                            self.video_srt_path.clear();
                            self.video_timed_lines.clear();
                            self.video_word_segments.clear();
                            self.last_video_export_path.clear();
                            self.video_status =
                                "Audiobook merged. Old subtitle files were cleared. Create SRT again."
                                    .to_string();
                        }
                    }
                    self.persist_state_to_disk();
                    self.qwen_status = match merge_error {
                        Some(err) => format!(
                            "Xu?t xong {} file, b? qua {}. Ch?a gh?p ???c audiobook: {}. Th? m?c: {}",
                            created, skipped, err, output_dir
                        ),
                        None => format!(
                            "Xu?t xong audio book. T?o {} file, b? qua {}. Audiobook: {}",
                            created,
                            skipped,
                            self.last_audiobook_path
                        ),
                    };
                    self.export_status = self.qwen_status.clone();
                    self.export_qwen_status = "Qwen ?? xong.".to_string();
                    self.export_gemini_status = "Gemini ?? xong.".to_string();
                    self.export_progress_done = created + skipped;
                    self.export_progress_total = created + skipped;
                    self.export_progress_label = format!(
                        "{} | ho?n t?t",
                        self.export_render_mode.as_str()
                    );
                    self.is_qwen_busy = false;
                    self.is_exporting_book = false;
                    clear_receiver = true;
                }
                QwenEvent::Error(err) => {
                    self.qwen_status = format!("L?i Qwen: {}", err);
                    self.export_status = if self.is_exporting_book {
                        format!("L?i export audio book: {}", err)
                    } else {
                        format!("Render line failed: {}", err)
                    };
                    let lowered = err.to_lowercase();
                    if lowered.contains("gemini") {
                        self.export_gemini_status = self.export_status.clone();
                    } else {
                        self.export_qwen_status = self.export_status.clone();
                    }
                    self.export_progress_label =
                        format!("{} | l?i", self.export_render_mode.as_str());
                    self.is_qwen_busy = false;
                    self.is_exporting_book = false;
                    clear_receiver = true;
                }
            }
        }

        if clear_receiver {
            self.qwen_rx = None;
        }
    }
}

impl eframe::App for TtsApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.handle_dropped_files(ctx);
        self.poll_tts_events();
        self.poll_analysis_events();
        self.poll_book_events();
        self.poll_character_events();
        self.poll_qwen_events();
        self.poll_line_render_events();
        self.poll_video_events();
        self.poll_srt_job_events();
        self.refresh_local_engines();
        let needs_repaint = self.is_fetching
            || self.is_playing
            || self.is_analyzing
            || self.is_formatting_book
            || self.is_analyzing_characters
            || self.is_qwen_busy
            || self.active_qwen_line_renders > 0
            || self.active_gemini_line_renders > 0
            || self.show_analysis_panel
            || self.is_exporting_video
            || self.show_video_window;
        if needs_repaint {
            ctx.request_repaint_after(Duration::from_millis(100));
        }

        let mut preview_clicked: Option<String> = None;
        let mut line_preview_clicked: Option<usize> = None;
        let mut cached_preview_clicked: Option<usize> = None;
        let mut speaker_edits: Vec<(usize, String)> = Vec::new();
        let mut text_edits: Vec<(usize, String)> = Vec::new();
        let hovering_files = ctx.input(|i| !i.raw.hovered_files.is_empty());
        let book_lines = parse_book_lines(&self.book_output);
        if self
            .selected_book_line
            .is_some_and(|index| index >= book_lines.len())
        {
            self.selected_book_line = None;
        }

        if self.show_project_picker {
            let projects = list_available_project_dirs().unwrap_or_default();
            egui::Window::new("Select Project")
                .collapsible(false)
                .resizable(true)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .default_size([560.0, 420.0])
                .show(ctx, |ui| {
                    ui.label("Select the project you want to work with.");
                    ui.label("Audio, SRT and video actions will stay inside this project.");
                    ui.separator();
                    ui.horizontal_wrapped(|ui| {
                        if ui.button("Create new project").clicked() {
                            match next_project_root_dir().and_then(|dir| {
                                ensure_project_structure(&dir)?;
                                self.load_project(&dir)
                            }) {
                                Ok(()) => {}
                                Err(err) => {
                                    self.status = format!("Could not create a new project: {}", err)
                                }
                            }
                        }
                        if !self.project_picker_selected.trim().is_empty()
                            && ui.button("Open selected project").clicked()
                        {
                            let path = PathBuf::from(self.project_picker_selected.trim());
                            if let Err(err) = self.load_project(&path) {
                                self.status = format!("Could not open project: {}", err);
                            }
                        }
                    });
                    ui.add_space(8.0);
                    egui::ScrollArea::vertical().max_height(300.0).show(ui, |ui| {
                        for path in projects {
                            let label = path
                                .file_name()
                                .and_then(|v| v.to_str())
                                .map(ToOwned::to_owned)
                                .unwrap_or_else(|| path.display().to_string());
                            let selected = self.project_picker_selected.eq_ignore_ascii_case(
                                &path.to_string_lossy(),
                            );
                            if ui.selectable_label(selected, label).clicked() {
                                self.project_picker_selected = path.to_string_lossy().to_string();
                            }
                        }
                    });
                    if !self.project_picker_selected.trim().is_empty() {
                        ui.separator();
                        ui.label(format!("Selected: {}", self.project_picker_selected));
                    }
                    if !self.status.trim().is_empty() {
                        ui.separator();
                        ui.label(format!("Status: {}", self.status));
                    }
                });
            return;
        }

        if self.show_project_rename {
            egui::Window::new("Rename Project")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("New name for the current project");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.project_rename_input)
                            .desired_width(320.0),
                    );
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Rename").clicked() {
                            match self.rename_current_project(&self.project_rename_input.clone()) {
                                Ok(()) => {
                                    self.show_project_rename = false;
                                }
                                Err(err) => {
                                    self.status = format!("Could not rename project: {}", err);
                                }
                            }
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_project_rename = false;
                        }
                    });
                });
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.heading("Instant Gemini Live TTS");
                ui.label(format!("Current project: {}", self.last_export_dir));
                ui.label(format!(
                    "Qwen Service: {}",
                    if self.qwen_service_ready {
                        "running"
                    } else {
                        "stopped"
                    }
                ));
                ui.label(format!(
                    "Model Qwen: {}",
                    if self.qwen_model_ready {
                        "ready"
                    } else if self.qwen_service_ready {
                        "loading"
                    } else {
                        "offline"
                    }
                ));
                if !self.srt_jobs.is_empty() {
                    ui.add_space(6.0);
                    egui::Frame::group(ui.style()).show(ui, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Background SRT jobs");
                            if ui.button("Clear finished").clicked() {
                                self.srt_jobs.retain(|job| !job.finished);
                            }
                        });
                        let mut stop_srt_job_id = None;
                        for job in &self.srt_jobs {
                            ui.separator();
                            let mut request_stop = false;
                            ui.horizontal_wrapped(|ui| {
                                ui.label(format!(
                                    "#{} | Project: {}",
                                    job.id, job.project_root
                                ));
                                if !job.finished && ui.button("Stop SRT").clicked() {
                                    request_stop = true;
                                }
                            });
                            if request_stop {
                                stop_srt_job_id = Some(job.id);
                            }
                            ui.label(format!("Audio: {}", job.audio_path));
                            if !job.srt_path.trim().is_empty() {
                                ui.label(format!("SRT: {}", job.srt_path));
                            }
                            ui.add(
                                egui::ProgressBar::new(job.progress_fraction.clamp(0.0, 1.0))
                                    .desired_width(ui.available_width())
                                    .text(job.progress_label.clone()),
                            );
                            ui.label(format!("Status: {}", job.status));
                        }
                        if let Some(job_id) = stop_srt_job_id {
                            self.stop_srt_job(job_id);
                        }
                    });
                }
                if self.is_exporting_video || !self.video_progress_label.trim().is_empty() {
                    ui.add_space(6.0);
                    egui::Frame::group(ui.style()).show(ui, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Background video job");
                            if ui.button(if self.show_video_window {
                                "Hide video export"
                            } else {
                                "Show video export"
                            }).clicked() {
                                self.show_video_window = !self.show_video_window;
                            }
                            if self.is_exporting_video && ui.button("Stop video job").clicked() {
                                self.stop_video_job();
                            }
                        });
                        ui.add(
                            egui::ProgressBar::new(self.video_progress_fraction.clamp(0.0, 1.0))
                                .desired_width(ui.available_width())
                                .text(self.video_progress_label.clone()),
                        );
                        ui.label(format!("Status: {}", self.video_status));
                        if !self.last_audiobook_path.trim().is_empty() {
                            ui.label(format!(
                                "Current project audiobook: {}",
                                self.last_audiobook_path
                            ));
                        }
                    });
                }
                ui.add_space(8.0);
                ui.horizontal_wrapped(|ui| {
                    if ui.button("Save app data").clicked() {
                        if self.persist_state_to_disk() {
                            self.status = "App data saved.".to_string();
                        }
                    }
                    if ui.button("Open project").clicked() {
                        self.show_project_picker = true;
                    }
                    if ui.button("Rename project").clicked() {
                        self.project_rename_input = Path::new(&self.last_export_dir)
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default()
                            .to_string();
                        self.show_project_rename = true;
                    }
                    if ui.button("New project").clicked() {
                        match next_project_root_dir().and_then(|dir| {
                            ensure_project_structure(&dir)?;
                            self.load_project(&dir)
                        }) {
                            Ok(()) => {}
                            Err(err) => {
                                self.status = format!("Could not create new project: {}", err)
                            }
                        }
                    }
                    if ui.button("Open data folder").clicked() {
                        match open_path_in_explorer(&app_data_dir()) {
                            Ok(()) => self.status = "Opened app-data folder.".to_string(),
                            Err(err) => {
                                self.status = format!("Could not open data folder: {}", err)
                            }
                        }
                    }
                    if ui.button("Start Qwen Service").clicked() {
                        self.launch_qwen_service();
                    }
                    if ui.button("Voice sample analysis").clicked() {
                        self.show_analysis_panel = true;
                    }
                });

                ui.add_space(10.0);
                egui::CollapsingHeader::new("Book -> Dialogue")
                    .default_open(true)
                    .show(ui, |ui| {
                        let missing_cached_lines = self.missing_cached_book_lines();
                        let missing_renderable_lines = self.missing_renderable_book_lines();
                        let can_export_full_audio =
                            !book_lines.is_empty() && missing_cached_lines.is_empty();

                        ui.horizontal_wrapped(|ui| {
                            if ui
                                .add_enabled(
                                    !self.is_formatting_book,
                                    egui::Button::new(if self.is_formatting_book {
                                        "Processing..."
                                    } else {
                                        "Split narrator / dialogue"
                                    }),
                                )
                                .clicked()
                            {
                                self.start_book_formatting();
                            }
                            if ui
                                .add_enabled(
                                    !self.is_analyzing_characters,
                                    egui::Button::new(if self.is_analyzing_characters {
                                        "Analyzing characters..."
                                    } else {
                                        "Analyze characters"
                                    }),
                                )
                                .clicked()
                            {
                                self.start_character_analysis();
                            }
                            if ui.button("Copy result").clicked() {
                                ui.ctx().copy_text(self.book_output.clone());
                                self.book_status = "Book result copied.".to_string();
                            }
                            if ui
                                .add_enabled(
                                    !book_lines.is_empty(),
                                    egui::Button::new(if self.show_official_export_settings {
                                        "Hide export"
                                    } else {
                                        "Export"
                                    }),
                                )
                                .clicked()
                            {
                                self.show_official_export_settings =
                                    !self.show_official_export_settings;
                            }
                            if ui
                                .button(if self.show_book_source {
                                    "Hide source text"
                                } else {
                                    "Show source text"
                                })
                                .clicked()
                            {
                                self.show_book_source = !self.show_book_source;
                            }
                            if ui
                                .button(if self.show_book_result {
                                    "Hide result"
                                } else {
                                    "Show result"
                                })
                                .clicked()
                            {
                                self.show_book_result = !self.show_book_result;
                            }
                        });
                        ui.horizontal_wrapped(|ui| {
                            if self.is_exporting_book
                                && ui.button("Stop audio export").clicked()
                            {
                                self.stop_book_export();
                            }
                            ui.separator();
                            if ui.button("Clear speech cache").clicked() {
                                self.clear_speech_cache();
                                self.export_status = "Speech cache cleared.".to_string();
                            }
                        });
                        ui.label("Export mode");
                        ui.horizontal(|ui| {
                            ui.radio_value(
                                &mut self.export_render_mode,
                                ExportRenderMode::PerLine,
                                "Per line",
                            );
                            ui.radio_value(
                                &mut self.export_render_mode,
                                ExportRenderMode::ByCharacter,
                                "By character",
                            );
                        });
                        if self.export_render_mode == ExportRenderMode::ByCharacter {
                            ui.label("Character grouping mode is experimental.");
                        }
                        if self.show_official_export_settings {
                            ui.group(|ui| {
                                ui.label("Per-speaker export settings");
                                ui.checkbox(&mut self.auto_merge_after_render, "Auto-merge audio");
                                let mut book_speakers = Vec::new();
                                let mut seen = std::collections::BTreeSet::new();
                                for line in &book_lines {
                                    let key = line.speaker.to_lowercase();
                                    if seen.insert(key) {
                                        book_speakers.push(line.speaker.clone());
                                    }
                                }
                                let character_names = self
                                    .characters
                                    .iter()
                                    .map(|item| item.name.clone())
                                    .collect::<Vec<_>>();
                                egui::Grid::new("speaker_export_settings_grid")
                                    .num_columns(5)
                                    .spacing([12.0, 6.0])
                                    .striped(false)
                                    .show(ui, |ui| {
                                for speaker in book_speakers {
                                    let mut mode = self.effective_speaker_export_mode(&speaker);
                                    let mut voice_character = self
                                        .effective_voice_character_for_speaker(&speaker)
                                        .unwrap_or_default();
                                        ui.add_sized([140.0, 22.0], egui::Label::new(&speaker));
                                        ui.radio_value(
                                            &mut mode,
                                            SpeakerExportMode::PerLine,
                                            "Per line",
                                        );
                                        ui.radio_value(
                                            &mut mode,
                                            SpeakerExportMode::Grouped,
                                            "Grouped",
                                        );
                                        ui.radio_value(
                                            &mut mode,
                                            SpeakerExportMode::Exclude,
                                            "Exclude",
                                        );
                                        egui::ComboBox::from_id_salt(format!("speaker_voice_map_{}", speaker))
                                    .selected_text(if voice_character.is_empty() {
                                            "Select voice..."
                                        } else {
                                            &voice_character
                                        })
                                        .show_ui(ui, |ui| {
                                            for name in &character_names {
                                                ui.selectable_value(
                                                    &mut voice_character,
                                                    name.clone(),
                                                    name,
                                                );
                                            }
                                        });
                                        ui.end_row();
                                    if mode != self.effective_speaker_export_mode(&speaker) {
                                        self.set_speaker_export_mode(&speaker, mode);
                                        let _ = self.persist_state_to_disk();
                                    }
                                    if Some(voice_character.clone()).filter(|item| !item.is_empty())
                                        != self.effective_voice_character_for_speaker(&speaker)
                                    {
                                        self.set_speaker_voice_character(&speaker, &voice_character);
                                        let _ = self.persist_state_to_disk();
                                    }
                                }
                                    });
                                ui.add_space(6.0);
                                if ui
                                    .add_enabled(
                                        !self.is_qwen_busy && !book_lines.is_empty(),
                                        egui::Button::new("Render all lines"),
                                    )
                                    .clicked()
                                {
                                    self.start_render_all_lines();
                                }
                                if ui
                                    .add_enabled(
                                        !self.is_qwen_busy && can_export_full_audio,
                                        egui::Button::new("Start export"),
                                    )
                                    .clicked()
                                {
                                    self.start_export_book_audio();
                                }
                                if ui
                                    .add_enabled(
                                        !missing_renderable_lines.is_empty(),
                                        egui::Button::new(if self.show_missing_audio_lines {
                                            "Hide unrendered lines"
                                        } else {
                                            "Show unrendered lines"
                                        }),
                                    )
                                    .clicked()
                                {
                                    self.show_missing_audio_lines =
                                        !self.show_missing_audio_lines;
                                }
                                if ui
                                    .add_enabled(
                                        !self.is_qwen_busy && !missing_renderable_lines.is_empty(),
                                        egui::Button::new("Render unrendered lines"),
                                    )
                                    .clicked()
                                {
                                    self.start_render_missing_lines();
                                }
                                if ui
                                    .add_enabled(
                                        !self.is_qwen_busy && !self.export_skipped_indices.is_empty(),
                                        egui::Button::new("Export skipped lines"),
                                    )
                                    .clicked()
                                {
                                    self.start_export_skipped_lines();
                                }
                            });
                            if !can_export_full_audio {
                                ui.add_space(4.0);
                                ui.label(format!(
                                    "Full audio export stays locked until every line has speech cache. Missing: {}",
                                    missing_cached_lines.len()
                                ));
                            }
                            if self.show_missing_audio_lines && !missing_renderable_lines.is_empty() {
                                ui.add_space(6.0);
                                ui.group(|ui| {
                                    ui.label(format!(
                                        "Unrendered lines: {}",
                                        missing_renderable_lines.len()
                                    ));
                                    egui::ScrollArea::vertical()
                                        .max_height(140.0)
                                        .show(ui, |ui| {
                                            for (index, line) in &missing_renderable_lines {
                                                ui.label(format!(
                                                    "{}. {}: {}",
                                                    index + 1,
                                                    line.speaker,
                                                    excerpt_for_error(&line.text)
                                                ));
                                            }
                                        });
                                });
                            }
                        }
                        ui.label(format!("Book status: {}", self.book_status));
                        ui.label(format!("Export status: {}", self.export_status));
                        if self.export_progress_total > 0 {
                            let fraction = self.export_progress_done as f32
                                / self.export_progress_total.max(1) as f32;
                            ui.add(
                                egui::ProgressBar::new(fraction.clamp(0.0, 1.0))
                                    .desired_width(ui.available_width())
                                    .text(self.export_progress_label.clone()),
                            );
                        }
                        if self.is_exporting_book || !self.export_qwen_status.is_empty() {
                            ui.label(format!("Qwen: {}", self.export_qwen_status));
                            ui.label(format!("Gemini: {}", self.export_gemini_status));
                        }
                        if !self.export_skip_details.is_empty() {
                            ui.horizontal_wrapped(|ui| {
                                ui.label(format!(
                                    "Skipped lines: {}",
                                    self.export_skip_details.len()
                                ));
                                if ui.button("Copy skipped errors").clicked() {
                                    ui.ctx().copy_text(self.export_skip_details.join("\n"));
                                    self.export_status =
                                        "Skipped-line errors copied.".to_string();
                                }
                                if ui
                                    .add_enabled(
                                        !self.is_qwen_busy && !self.export_skipped_indices.is_empty(),
                                        egui::Button::new("Render skipped lines"),
                                    )
                                    .clicked()
                                {
                                    self.start_export_skipped_lines();
                                }
                            });
                            egui::ScrollArea::vertical()
                                .max_height(120.0)
                                .show(ui, |ui| {
                                    for item in &self.export_skip_details {
                                        ui.label(item);
                                    }
                                });
                        }
                        if self.show_book_source {
                            ui.label("Source text");
                            ui.add_sized(
                                [ui.available_width(), 180.0],
                                egui::TextEdit::multiline(&mut self.book_input)
                                    .desired_width(f32::INFINITY)
                                    .hint_text("Paste story text or book text here..."),
                            );
                        }
                        if self.show_book_result {
                            ui.label("Narrator / character result");
                            ui.add_sized(
                                [ui.available_width(), 220.0],
                                egui::TextEdit::multiline(&mut self.book_output)
                                    .desired_width(f32::INFINITY)
                                    .hint_text("Parsed narrator / character output will appear here."),
                            );
                        }
                        ui.label(format!("Audiobook export folder: {}", self.last_export_dir));
                        if !self.last_audiobook_path.is_empty() {
                            ui.label(format!("Merged audiobook file: {}", self.last_audiobook_path));
                        }
                        ui.horizontal_wrapped(|ui| {
                            if ui.button("Open export folder").clicked() {
                                match open_path_in_explorer(Path::new(&self.last_export_dir)) {
                                    Ok(()) => {
                                        self.qwen_status = "Opened audio export folder.".to_string()
                                    }
                                    Err(err) => {
                                        self.qwen_status =
                                            format!("Could not open export folder: {}", err)
                                    }
                                }
                            }
                            if !self.last_audiobook_path.is_empty()
                                && ui.button("Open audiobook file").clicked()
                            {
                                match open_in_browser(&self.last_audiobook_path) {
                                    Ok(()) => {
                                        self.qwen_status = "Opened audiobook file.".to_string()
                                    }
                                    Err(err) => {
                                        self.qwen_status =
                                            format!("Could not open audiobook: {}", err)
                                    }
                                }
                            }
                            if ui.button("Rebuild audiobook").clicked() {
                                match self.rebuild_audiobook_from_cache() {
                                    Ok(()) => {
                                        self.export_status = format!(
                                            "Rebuilt audiobook: {}",
                                            self.last_audiobook_path
                                        );
                                    }
                                    Err(err) => {
                                        self.export_status =
                                            format!("Could not rebuild audiobook: {}", err);
                                    }
                                }
                            }
                            if ui
                                .button(if self.show_video_window {
                                    "Hide video export"
                                } else {
                                    "Export video"
                                })
                                .clicked()
                            {
                                self.show_video_window = !self.show_video_window;
                            }
                            ui.label("The app keeps only the merged audiobook file.");
                        });
                        ui.add_space(8.0);
                            ui.label("Parsed dialogue lines");
                        egui::ScrollArea::vertical()
                            .id_salt("book_lines_scroll")
                            .max_height(280.0)
                            .show(ui, |ui| {
                                if book_lines.is_empty() {
                                    ui.label("No dialogue lines available.");
                                } else {
                                    for (index, line) in book_lines.iter().enumerate() {
                                        ui.group(|ui| {
                                            let selected = self.selected_book_line == Some(index);
                                            let mut line_gain = self.line_volume_percent(index);
                                            let mut speaker_name = line.speaker.clone();
                                            ui.horizontal_wrapped(|ui| {
                                                let header = format!("{}.", index + 1);
                                                if ui.selectable_label(selected, header).clicked() {
                                                    self.selected_book_line = Some(index);
                                                }
                                                let response = ui.add(
                                                    egui::TextEdit::singleline(&mut speaker_name)
                                                        .desired_width(140.0),
                                                );
                                                if response.changed() {
                                                    speaker_edits.push((index, speaker_name.clone()));
                                                }
                                                if ui.button("Preview").clicked() {
                                                    line_preview_clicked = Some(index);
                                                }
                                                if ui.button("Render line").clicked() {
                                                    self.start_render_book_line(index);
                                                }
                                                if self
                                                    .exported_speeches
                                                    .iter()
                                                    .any(|item| item.index == index)
                                                    && ui.button("Nghe cache").clicked()
                                                {
                                                    cached_preview_clicked = Some(index);
                                                }
                                                ui.label("Line volume");
                                                if ui
                                                    .add(
                                                        egui::Slider::new(&mut line_gain, 40..=220)
                                                            .suffix("%"),
                                                    )
                                                    .changed()
                                                {
                                                    self.set_line_volume_percent(index, line_gain);
                                                    let final_gain =
                                                        self.effective_gain_percent_for_line(index, &line.speaker);
                                                    match self.apply_gain_to_cached_speech(index, final_gain) {
                                                        Ok(true) => {
                                                            let _ = self.rebuild_audiobook_from_cache();
                                                            self.export_status = format!(
                                                                "Applied line volume {} = {}% to cache.",
                                                                index + 1,
                                                                line_gain
                                                            );
                                                        }
                                                        Ok(false) => {
                                                            self.export_status = format!(
                                                                "Set line volume {} = {}%. The app will apply it when cache exists or after render.",
                                                                index + 1,
                                                                line_gain
                                                            );
                                                        }
                                                        Err(err) => {
                                                            self.export_status = format!(
                                                                "Saved line volume {} but could not apply it to cache yet: {}",
                                                                index + 1,
                                                                err
                                                            );
                                                        }
                                                    }
                                                }
                                            });
                                            let mut line_text = line.text.clone();
                                            let text_response = ui.add(
                                                egui::TextEdit::multiline(&mut line_text)
                                                    .desired_width(ui.available_width())
                                                    .desired_rows(3),
                                            );
                                            if text_response.changed() {
                                                text_edits.push((index, line_text));
                                            }
                                            if text_response.clicked() {
                                                self.selected_book_line = Some(index);
                                            }
                                        });
                                        ui.add_space(4.0);
                                    }
                                }
                            });
                    });

                ui.add_space(10.0);
                egui::CollapsingHeader::new("Qwen Character Library")
                    .default_open(true)
                    .show(ui, |ui| {
                        ui.horizontal_wrapped(|ui| {
                            if ui.button("Add character").clicked() {
                                self.add_character();
                            }
                            if ui
                                .add_enabled(!self.is_qwen_busy, egui::Button::new("Get ref text with Gemini"))
                                .clicked()
                            {
                                self.start_character_ref_text_extraction();
                            }
                            if ui
                                .add_enabled(!self.is_qwen_busy, egui::Button::new("Preview character voice"))
                                .clicked()
                            {
                                self.start_character_preview();
                            }
                            if ui.button("Save character").clicked() {
                                self.save_character_record();
                            }
                            if ui.button("Delete character").clicked() {
                                self.delete_selected_character();
                            }
                        });
                        ui.label(format!("Character status: {}", self.character_status));
                        ui.horizontal(|ui| {
                            ui.vertical(|ui| {
                                ui.set_min_width(380.0);
                                ui.set_max_width(420.0);
                                ui.label("Character list");
                                egui::Frame::group(ui.style()).show(ui, |ui| {
                                    ui.set_min_height(520.0);
                                    egui::ScrollArea::vertical()
                                        .id_salt("character_library_scroll")
                                        .max_height(500.0)
                                        .auto_shrink([false, false])
                                        .show(ui, |ui| {
                                            if self.characters.is_empty() {
                                                ui.label("No saved character is mapped.");
                                            } else {
                                                ui.label(format!("Total characters: {}", self.characters.len()));
                                                ui.separator();
                                                let items: Vec<(usize, String)> = self
                                                    .characters
                                                    .iter()
                                                    .enumerate()
                                                    .map(|(idx, item)| (idx, item.name.clone()))
                                                    .collect();
                                                for (idx, name) in items {
                                                    let selected =
                                                        self.selected_character == Some(idx);
                                                    if ui.selectable_label(selected, name).clicked()
                                                    {
                                                        self.load_selected_character(idx);
                                                    }
                                                }
                                            }
                                        });
                                });
                            });

                            ui.separator();

                            ui.vertical(|ui| {
                                ui.set_width(ui.available_width());
                                ui.label("Character details");
                                ui.group(|ui| {
                                    ui.set_min_height(72.0);
                                    ui.label("Drag and drop a reference audio file into the app.");
                                    ui.label("The dropped audio will be assigned to the current character.");
                                    if !self.character_ref_audio_input.trim().is_empty() {
                                        ui.monospace(self.character_ref_audio_input.clone());
                                    }
                                });
                                ui.add_sized(
                                    [ui.available_width(), 28.0],
                                    egui::TextEdit::singleline(&mut self.character_name_input)
                                        .hint_text("Character name"),
                                );
                                ui.label("Engine");
                                ui.horizontal(|ui| {
                                    ui.radio_value(
                                        &mut self.character_tts_engine_input,
                                        "qwen".to_string(),
                                        "Qwen clone",
                                    );
                                    ui.radio_value(
                                        &mut self.character_tts_engine_input,
                                        "gemini".to_string(),
                                        "Gemini Live",
                                    );
                                });
                                if self.character_tts_engine_input == "gemini" {
                                    ui.label("Gemini voice");
                                    egui::ComboBox::from_id_salt("character_gemini_voice")
                                        .selected_text(&self.character_gemini_voice_input)
                                        .show_ui(ui, |ui| {
                                            for voice in MALE_VOICES.iter().chain(FEMALE_VOICES.iter()) {
                                                ui.selectable_value(
                                                    &mut self.character_gemini_voice_input,
                                                    (*voice).to_string(),
                                                    *voice,
                                                );
                                            }
                                        });
                                    ui.label("Gemini speech speed");
                                    ui.horizontal(|ui| {
                                        ui.radio_value(
                                            &mut self.character_gemini_speed_input,
                                            SpeechSpeed::Slow,
                                            "Slow",
                                        );
                                        ui.radio_value(
                                            &mut self.character_gemini_speed_input,
                                            SpeechSpeed::Normal,
                                            "Normal",
                                        );
                                        ui.radio_value(
                                            &mut self.character_gemini_speed_input,
                                            SpeechSpeed::Fast,
                                            "Fast",
                                        );
                                    });
                                    ui.add_sized(
                                        [ui.available_width(), 90.0],
                                        egui::TextEdit::multiline(
                                            &mut self.character_gemini_style_prompt_input,
                                        )
                                        .desired_width(f32::INFINITY)
                                        .hint_text("Gemini Live style prompt"),
                                    );
                                }
                                if let Some(selected_index) = self.selected_character {
                                    if let Some(character) = self.characters.get(selected_index) {
                                        let mut volume_percent = character.volume_percent;
                                        ui.label("Character volume");
                                        if ui
                                            .add(
                                                egui::Slider::new(&mut volume_percent, 40..=220)
                                                    .suffix("%"),
                                            )
                                            .changed()
                                        {
                                            let selected_name = self.character_name_input.clone();
                                            if let Some(item) = self.characters.get_mut(selected_index) {
                                                item.volume_percent = volume_percent;
                                            }
                                            let _ = self.persist_state_to_disk();
                                            match self.apply_character_volume_to_cached_speeches(
                                                &selected_name,
                                            ) {
                                                Ok(changed) if changed > 0 => {
                                                    self.character_status = format!(
                                                        "Set character volume '{}' = {}% and applied it to {} cached lines.",
                                                        selected_name,
                                                        volume_percent,
                                                        changed
                                                    );
                                                }
                                                Ok(_) => {
                                                    self.character_status = format!(
                                                        "Set character volume '{}' = {}%. Future renders will use this value.",
                                                        selected_name,
                                                        volume_percent
                                                    );
                                                }
                                                Err(err) => {
                                                    self.character_status = format!(
                                                        "Saved character volume '{}' but could not apply it to cache yet: {}",
                                                        selected_name,
                                                        err
                                                    );
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    ui.label("Save the character first before adjusting character volume.");
                                }
                                ui.add_sized(
                                    [ui.available_width(), 90.0],
                                    egui::TextEdit::multiline(&mut self.character_description_input)
                                        .desired_width(f32::INFINITY)
                                        .hint_text("English voice description"),
                                );
                                ui.add_sized(
                                    [ui.available_width(), 120.0],
                                    egui::TextEdit::multiline(&mut self.character_ref_text_input)
                                        .desired_width(f32::INFINITY)
                                        .hint_text("Qwen reference text. Gemini can fill this for you."),
                                );
                                ui.add_sized(
                                    [ui.available_width(), 28.0],
                                    egui::TextEdit::singleline(&mut self.character_ref_audio_input)
                                        .hint_text("Reference audio path"),
                                );
                                if ui.button("Copy ref text").clicked() {
                                    ui.ctx().copy_text(self.character_ref_text_input.clone());
                                    self.character_status = "Reference text copied.".to_string();
                                }
                            });
                        });
                    });

                ui.add_space(10.0);
                egui::CollapsingHeader::new("Qwen3")
                    .default_open(true)
                    .show(ui, |ui| {
                        ui.label(format!(
                            "Qwen service status: {}",
                            if self.qwen_service_ready {
                                "running"
                            } else {
                                "stopped"
                            }
                        ));
                        ui.label(format!(
                            "Model Qwen: {}",
                            if self.qwen_model_ready {
                                "ready"
                            } else if self.qwen_service_ready {
                                "loading"
                            } else {
                                "offline"
                            }
                        ));
                        for (index, url) in QWEN_SERVICE_URLS.iter().enumerate() {
                            let (service_ready, model_ready) = self
                                .qwen_service_healths
                                .get(index)
                                .copied()
                                .unwrap_or((false, false));
                            let port_label = url.rsplit(':').next().unwrap_or(url);
                            let service_text = if service_ready { "running" } else { "stopped" };
                            let model_text = if model_ready {
                                "ready"
                            } else if service_ready {
                                "loading"
                            } else {
                                "offline"
                            };
                            ui.label(format!(
                                "Service {}: {} | Model {}",
                                port_label, service_text, model_text
                            ));
                        }
                        ui.horizontal_wrapped(|ui| {
                            if ui.button("Start Qwen Service").clicked() {
                                self.launch_qwen_service();
                            }
                        });
                        ui.add_space(6.0);
                        ui.label("Pause between audiobook lines");
                        ui.add(
                            egui::Slider::new(&mut self.audiobook_pause_ms, 180..=700)
                                .suffix(" ms"),
                        );
                        ui.add_space(6.0);
                        ui.label("Ng�n ng? Qwen");
                        egui::ComboBox::from_id_salt("qwen_language")
                            .selected_text(&self.qwen_language)
                            .show_ui(ui, |ui| {
                                for language in QWEN_SUPPORTED_LANGUAGES {
                                    ui.selectable_value(
                                        &mut self.qwen_language,
                                        (*language).to_string(),
                                        *language,
                                    );
                                }
                            });
                        ui.label("Ch? d? clone");
                        ui.horizontal(|ui| {
                            ui.radio_value(
                                &mut self.qwen_xvector_only,
                                false,
                                "Ref audio + ref text",
                            );
                            ui.radio_value(
                                &mut self.qwen_xvector_only,
                                true,
                                "Use x-vector only",
                            );
                        });
                        ui.label(format!("Qwen status: {}", self.qwen_status));
                        if let Some(index) = self.selected_character {
                            if let Some(character) = self.characters.get(index) {
                                ui.separator();
                                ui.label(format!("Selected character: {}", character.name));
                                ui.label(format!(
                                    "Engine: {}",
                                    if character.tts_engine == "gemini" {
                                        "Gemini Live"
                                    } else {
                                        "Qwen clone"
                                    }
                                ));
                                if character.tts_engine == "gemini" {
                                    ui.label(format!("Gemini voice: {}", character.gemini_voice));
                                }
                                if !character.ref_audio_path.is_empty() {
                                    ui.label(format!("Ref audio: {}", character.ref_audio_path));
                                }
                                if !character.ref_text.is_empty() {
                                    ui.label("Ref text already exists in the library.");
                                }
                            }
                        } else {
                            ui.label("Select a character from the library to reuse ref audio and ref text.");
                        }
                    });

                ui.add_space(10.0);
                egui::CollapsingHeader::new("Gemini TTS")
                    .default_open(false)
                    .show(ui, |ui| {
                        ui.label("Gemini API Key");
                        let api_response = ui.add_sized(
                            [ui.available_width(), 28.0],
                            egui::TextEdit::singleline(&mut self.api_key)
                                .password(true)
                                .hint_text("AIza..."),
                        );
                        if api_response.changed() {
                            let _ = self.persist_state_to_disk();
                        }

                        ui.add_space(6.0);
                        ui.label("Selected voice");
                        ui.add_sized(
                            [ui.available_width(), 28.0],
                            egui::TextEdit::singleline(&mut self.voice_name)
                                .hint_text(DEFAULT_VOICE),
                        );

                        ui.add_space(6.0);
                        ui.label("T?c d? d?c");
                        ui.horizontal(|ui| {
                            ui.radio_value(&mut self.speed, SpeechSpeed::Slow, "Slow");
                            ui.radio_value(&mut self.speed, SpeechSpeed::Normal, "Normal");
                            ui.radio_value(&mut self.speed, SpeechSpeed::Fast, "Nhanh");
                        });
                        ui.label(format!("Selected: {}", self.speed.as_str()));

                        ui.add_space(6.0);
                        ui.label("Prompt gi?ng di?u / c�ch d?c");
                        ui.add_sized(
                            [ui.available_width(), 110.0],
                            egui::TextEdit::multiline(&mut self.style_instruction)
                                .desired_width(f32::INFINITY)
                                .hint_text("Example: Read naturally, clearly, with a storytelling rhythm."),
                        );

                        ui.add_space(6.0);
                        ui.label("N?i dung preview voice");
                        ui.add_sized(
                            [ui.available_width(), 70.0],
                            egui::TextEdit::multiline(&mut self.preview_text)
                                .desired_width(f32::INFINITY)
                                .hint_text(DEFAULT_PREVIEW_TEXT),
                        );

                        ui.add_space(6.0);
                        ui.label("Van b?n ch�nh d? d?c");
                        ui.add_sized(
                            [ui.available_width(), 180.0],
                            egui::TextEdit::multiline(&mut self.text)
                                .desired_width(f32::INFINITY)
                                .hint_text("Enter the main text to read here..."),
                        );

                        ui.add_space(10.0);
                        ui.horizontal_wrapped(|ui| {
                            if ui
                                .add_enabled(
                                    !self.is_fetching,
                                    egui::Button::new(if self.is_fetching {
                                        "Processing..."
                                    } else {
                                        "Speak text"
                                    }),
                                )
                                .clicked()
                            {
                                self.start_speak_main();
                            }
                            if ui.button("Stop").clicked() {
                                self.stop_audio();
                                self.status = "Stopped.".to_string();
                            }
                            if ui.button("Export audio (WAV)").clicked() {
                                self.export_last_audio();
                            }
                            if ui.button("Open voice sample analysis").clicked() {
                                self.show_analysis_panel = true;
                            }
                        });

                        ui.label(format!("Default export folder: {}", EXPORT_DIR));
                        ui.label(format!("TTS status: {}", self.status));

                        ui.add_space(10.0);
                        ui.separator();
                        ui.label("Voice preview");
                        ui.columns(2, |cols| {
                            cols[0].label("Nam");
                            egui::ScrollArea::vertical()
                                .id_salt("male_voices_scroll")
                                .max_height(180.0)
                                .show(&mut cols[0], |ui| {
                                    for voice in MALE_VOICES {
                                        ui.push_id(format!("male_{voice}"), |ui| {
                                            let selected = self.voice_name == *voice;
                                            if ui.selectable_label(selected, *voice).clicked() {
                                                preview_clicked = Some((*voice).to_string());
                                            }
                                            if ui.small_button("Nghe").clicked() {
                                                preview_clicked = Some((*voice).to_string());
                                            }
                                        });
                                    }
                                });

                            cols[1].label("Female");
                            egui::ScrollArea::vertical()
                                .id_salt("female_voices_scroll")
                                .max_height(180.0)
                                .show(&mut cols[1], |ui| {
                                    for voice in FEMALE_VOICES {
                                        ui.push_id(format!("female_{voice}"), |ui| {
                                            let selected = self.voice_name == *voice;
                                            if ui.selectable_label(selected, *voice).clicked() {
                                                preview_clicked = Some((*voice).to_string());
                                            }
                                            if ui.small_button("Nghe").clicked() {
                                                preview_clicked = Some((*voice).to_string());
                                            }
                                        });
                                    }
                                });
                        });
                    });
            });
        });

        if self.show_analysis_panel {
            let drop_fill = if hovering_files {
                egui::Color32::from_rgb(226, 239, 255)
            } else {
                egui::Color32::from_rgb(246, 246, 246)
            };
            let mut analysis_window_open = self.show_analysis_panel;

            egui::Window::new("Voice Sample Analysis")
                .open(&mut analysis_window_open)
                .default_size([720.0, 620.0])
                .vscroll(true)
                .show(ctx, |ui| {
                    ui.label(
                        "K�o th? m?t do?n voice ng?n c?a m?t ngu?i v�o d�y. Gemini s? ph�n t�ch nh?p d?c, d? d�y gi?ng, d? s�ng/t?i, ng?t ngh? v� g?i � prompt d? b?n paste v�o TTS.",
                    );

                    egui::Frame::group(ui.style())
                        .fill(drop_fill)
                        .show(ui, |ui| {
                            ui.set_min_height(90.0);
                            ui.vertical_centered(|ui| {
                                ui.label("K�o v� th? file audio v�o d�y");
                                ui.label("Supported: mp3, wav, m4a, flac, ogg, webm, aac");
                                ui.label("Use a short 5-20 second clip with one clear speaker.");
                                if let Some(path) = &self.dropped_audio_path {
                                    ui.monospace(path.to_string_lossy());
                                }
                            });
                        });

                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add_enabled(
                                !self.is_analyzing && self.dropped_audio_path.is_some(),
                                egui::Button::new(if self.is_analyzing {
                                    "Analyzing..."
                                } else {
                                    "Analyze voice"
                                }),
                            )
                            .clicked()
                        {
                            self.start_voice_analysis();
                        }

                            if ui.button("D�ng prompt n�y cho TTS").clicked() {
                            if !self.analysis_result.tts_prompt.trim().is_empty() {
                                self.style_instruction = self.analysis_result.tts_prompt.clone();
                                self.status =
                                    "Loaded the analysis prompt into the voice style prompt field."
                                        .to_string();
                            }
                        }

                        if ui.button("Copy prompt").clicked() {
                            ui.ctx().copy_text(self.analysis_result.tts_prompt.clone());
                            self.analysis_status = "Copied prompt to clipboard.".to_string();
                        }
                    });

                    ui.label(format!("Analysis status: {}", self.analysis_status));

                    ui.add_space(6.0);
                    ui.label("Transcript t? audio");
                    ui.add_sized(
                        [ui.available_width(), 100.0],
                        egui::TextEdit::multiline(&mut self.analysis_result.transcript)
                            .desired_width(f32::INFINITY)
                            .interactive(false)
                            .hint_text("Gemini transcript will appear here."),
                    );

                    ui.add_space(6.0);
                    ui.label("T�m t?t c?u tr�c gi?ng");
                    ui.add_sized(
                        [ui.available_width(), 140.0],
                        egui::TextEdit::multiline(&mut self.analysis_result.style_summary)
                            .desired_width(f32::INFINITY)
                            .interactive(false)
                            .hint_text("Summary of rhythm, weight, breathiness, and pauses..."),
                    );

                    ui.add_space(6.0);
                    ui.label("Prompt d? paste v�o TTS");
                    ui.add_sized(
                        [ui.available_width(), 140.0],
                        egui::TextEdit::multiline(&mut self.analysis_result.tts_prompt)
                            .desired_width(f32::INFINITY)
                            .hint_text("Prompt to copy into the voice style / reading prompt field."),
                    );
                });
            self.show_analysis_panel = analysis_window_open;
        }

        if self.show_video_window {
            self.ensure_video_preview_texture(ctx);
            ensure_video_preview_font(ctx, &self.video_font_name);
            if self.video_timed_lines.is_empty() {
                let auto_srt_path = if !self.video_srt_path.trim().is_empty() {
                    Some(PathBuf::from(self.video_srt_path.trim()))
                } else if !self.last_export_dir.trim().is_empty() {
                    latest_srt_in_project(Path::new(&self.last_export_dir))
                } else {
                    None
                };
                if let Some(path) = auto_srt_path {
                    if path.exists() {
                        let _ = self.load_video_srt_from_path(&path);
                    }
                }
            }
            let preview_lines_all = parse_book_lines(&self.book_output);
            if !preview_lines_all.is_empty() && self.video_preview_line_index >= preview_lines_all.len() {
                self.video_preview_line_index = preview_lines_all.len() - 1;
            }
            egui::SidePanel::right("video_export_panel")
                .resizable(true)
                .default_width(560.0)
                .min_width(420.0)
                .show(ctx, |ui| {
                    ui.heading("Video Export");
                    ui.horizontal_wrapped(|ui| {
                        if ui.button("Hide video export").clicked() {
                            self.show_video_window = false;
                        }
                    });
                    ui.add_space(6.0);
                    ui.label(format!("Video status: {}", self.video_status));
                    let current_project_audio = if !self.last_audiobook_path.trim().is_empty() {
                        self.last_audiobook_path.trim().to_string()
                    } else {
                        "(none)".to_string()
                    };
                    ui.label(format!("Current project audiobook: {}", current_project_audio));
                    let current_project_root = if self.last_export_dir.trim().is_empty() {
                        None
                    } else {
                        Some(normalize_project_root(Path::new(&self.last_export_dir)))
                    };
                    if let Some(root) = current_project_root {
                        let gemini_audio = project_subtitle_dir(&root).join("audiobook_for_gemini.mp3");
                        ui.label(format!(
                            "Current SRT source audio: {}",
                            if gemini_audio.exists() {
                                gemini_audio.display().to_string()
                            } else {
                                "(not created yet)".to_string()
                            }
                        ));
                    }
                    if !self.video_srt_path.trim().is_empty() {
                        ui.label(format!("Current SRT: {}", self.video_srt_path));
                    }
                    egui::Frame::group(ui.style()).show(ui, |ui| {
                        ui.label("Drop an SRT file here, or paste a path. The app will auto-load it if it exists.");
                        ui.horizontal_wrapped(|ui| {
                            let srt_response = ui.add_sized(
                                [ui.available_width() - 100.0, 28.0],
                                egui::TextEdit::singleline(&mut self.video_srt_path)
                                    .hint_text("D:\\audio\\...\\captions.srt"),
                            );
                            if srt_response.changed() {
                                let path = PathBuf::from(self.video_srt_path.trim());
                                if path.exists() {
                                    let _ = self.load_video_srt_from_path(&path);
                                }
                                let _ = self.persist_state_to_disk();
                            }
                            if ui.button("Load SRT").clicked() {
                                let path = PathBuf::from(self.video_srt_path.trim());
                                match self.load_video_srt_from_path(&path) {
                                    Ok(()) => {}
                                    Err(err) => {
                                        self.video_status = format!("Could not load SRT: {}", err);
                                    }
                                }
                            }
                        });
                    });
                    ui.horizontal_wrapped(|ui| {
                        ui.label("Background image");
                        let bg_response = ui.add_sized(
                            [ui.available_width() - 120.0, 28.0],
                            egui::TextEdit::singleline(&mut self.video_background_path)
                                .hint_text("Drop a background image here or paste a path."),
                        );
                        if bg_response.changed() {
                            self.video_preview_texture = None;
                            self.video_preview_texture_path.clear();
                            let _ = self.persist_state_to_disk();
                        }
                    });
                    let old_tag_position = self.video_tag_position;
                    ui.horizontal_wrapped(|ui| {
                        ui.label("Video tag");
                        let tag_response = ui.add_sized(
                            [280.0, 28.0],
                            egui::TextEdit::singleline(&mut self.video_corner_tag)
                                .hint_text("Prologue / Chapter 1 / Epilogue"),
                        );
                        if tag_response.changed() {
                            let _ = self.persist_state_to_disk();
                        }
                        ui.label("Tag font");
                        if ui
                            .add(egui::Slider::new(&mut self.video_tag_font_size, 36..=120))
                            .changed()
                        {
                            let _ = self.persist_state_to_disk();
                        }
                        if ui
                            .checkbox(&mut self.video_tag_background_enabled, "Tag background")
                            .changed()
                        {
                            let _ = self.persist_state_to_disk();
                        }
                        ui.radio_value(
                            &mut self.video_tag_position,
                            VideoTagPosition::TopLeft,
                            "Top-left",
                        );
                        ui.radio_value(
                            &mut self.video_tag_position,
                            VideoTagPosition::TopCenter,
                            "Top-center",
                        );
                        ui.label("Subtitle lead");
                        if ui
                            .add(
                                egui::Slider::new(
                                    &mut self.video_subtitle_lead_seconds,
                                    0.0..=1.5,
                                )
                                .step_by(0.05),
                            )
                            .changed()
                        {
                            let current_srt = self.video_srt_path.trim().to_string();
                            let _ = self.persist_state_to_disk();
                            if !current_srt.is_empty() {
                                let path = PathBuf::from(current_srt);
                                if path.exists() {
                                    let _ = self.load_video_srt_from_path(&path);
                                }
                            }
                        }
                    });
                    if self.video_tag_position != old_tag_position {
                        let _ = self.persist_state_to_disk();
                    }
                    ui.horizontal_wrapped(|ui| {
                        ui.label("Font");
                        let mut font_changed = false;
                        egui::ComboBox::from_id_salt("video_font_name")
                            .selected_text(&self.video_font_name)
                            .show_ui(ui, |ui| {
                                for font_name in VIDEO_FONT_OPTIONS {
                                    if ui.selectable_value(
                                        &mut self.video_font_name,
                                        (*font_name).to_string(),
                                        *font_name,
                                    ).changed() {
                                        font_changed = true;
                                    }
                                }
                            });
                        if font_changed {
                            ensure_video_preview_font(ctx, &self.video_font_name);
                            let _ = self.persist_state_to_disk();
                        }
                        ui.label("Text color");
                        let mut picked_color =
                            parse_hex_color(&self.video_text_color).unwrap_or(egui::Color32::WHITE);
                        if egui::color_picker::color_edit_button_srgba(
                            ui,
                            &mut picked_color,
                            egui::color_picker::Alpha::Opaque,
                        )
                        .changed()
                        {
                            self.video_text_color = format!(
                                "#{:02X}{:02X}{:02X}",
                                picked_color.r(),
                                picked_color.g(),
                                picked_color.b()
                            );
                            let _ = self.persist_state_to_disk();
                        }
                        ui.monospace(self.video_text_color.clone());
                        ui.label("Text box opacity");
                        if ui
                            .add(egui::Slider::new(&mut self.video_card_opacity, 40..=240))
                            .changed()
                        {
                            let _ = self.persist_state_to_disk();
                        }
                        ui.label("Font size");
                        if ui
                            .add(egui::Slider::new(&mut self.video_font_size, 24..=88))
                            .changed()
                        {
                            let _ = self.persist_state_to_disk();
                        }
                    });
                    if !preview_lines_all.is_empty() {
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Preview line");
                            let max_index = preview_lines_all.len().saturating_sub(1);
                            if ui
                                .add(
                                    egui::Slider::new(
                                        &mut self.video_preview_line_index,
                                        0..=max_index,
                                    )
                                    .custom_formatter(|value, _| format!("{}", value as usize + 1)),
                                )
                                .changed()
                            {
                                self.selected_book_line = Some(self.video_preview_line_index);
                            }
                        });
                    }
                    if !self.video_timed_lines.is_empty() {
                        let total_seconds = self
                            .video_timed_lines
                            .last()
                            .map(|line| line.end_seconds)
                            .unwrap_or(0.0);
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Preview seek");
                            let mut seek = self.video_preview_seek_seconds.min(total_seconds);
                            let response = ui.add(
                                egui::Slider::new(&mut seek, 0.0..=total_seconds.max(0.1))
                                    .show_value(false),
                            );
                            ui.label(format!(
                                "{:.1}s / {:.1}s",
                                seek.min(total_seconds),
                                total_seconds
                            ));
                            if response.changed() {
                                self.video_preview_seek_seconds = seek;
                                self.video_preview_started_at = None;
                                self.video_preview_line_index = self.current_video_preview_line_index();
                            }
                        });
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Preview length");
                            if ui
                                .add(
                                    egui::Slider::new(
                                        &mut self.video_preview_clip_duration_seconds,
                                        3..=90,
                                    )
                                    .suffix("s"),
                                )
                                .changed()
                            {
                                let _ = self.persist_state_to_disk();
                            }
                        });
                    }
                    ui.horizontal_wrapped(|ui| {
                        ui.label("Resolution");
                        if ui.radio_value(
                            &mut self.video_resolution,
                            VideoResolution::Hd720,
                            "720p",
                        ).changed() {
                            let _ = self.persist_state_to_disk();
                        }
                        if ui.radio_value(
                            &mut self.video_resolution,
                            VideoResolution::FullHd1080,
                            "1080p",
                        ).changed() {
                            let _ = self.persist_state_to_disk();
                        }
                        ui.label("FPS");
                        if ui
                            .radio_value(&mut self.video_frame_rate, VideoFrameRate::Fps30, "30")
                            .changed()
                        {
                            let _ = self.persist_state_to_disk();
                        }
                        if ui
                            .radio_value(&mut self.video_frame_rate, VideoFrameRate::Fps60, "60")
                            .changed()
                        {
                            let _ = self.persist_state_to_disk();
                        }
                    });
                    ui.horizontal_wrapped(|ui| {
                        ui.label("Highlight");
                        if ui
                            .radio_value(
                                &mut self.video_word_highlight_enabled,
                                false,
                                "Sentence",
                            )
                            .changed()
                        {
                            self.video_word_segments.clear();
                            let _ = self.persist_state_to_disk();
                        }
                        if ui
                            .radio_value(
                                &mut self.video_word_highlight_enabled,
                                true,
                                "Word",
                            )
                            .changed()
                        {
                            let _ = self.persist_state_to_disk();
                        }
                        ui.label(if self.video_word_highlight_enabled {
                            "Word mode: Create SRT will call Gemini for an extra word-timing pass."
                        } else {
                            "Sentence mode: Create SRT uses sentence timing only, faster."
                        });
                    });

                    let preview_width = ui.available_width().min(820.0);
                    let preview_height = preview_width * 9.0 / 16.0;
                    let (preview_rect, _) = ui.allocate_exact_size(
                        egui::vec2(preview_width, preview_height),
                        egui::Sense::hover(),
                    );
                    let painter = ui.painter_at(preview_rect);
                    painter.rect_filled(preview_rect, 12.0, egui::Color32::from_rgb(18, 18, 18));
                    if let Some(texture) = &self.video_preview_texture {
                        painter.image(
                            texture.id(),
                            preview_rect,
                            egui::Rect::from_min_max(
                                egui::Pos2::new(0.0, 0.0),
                                egui::Pos2::new(1.0, 1.0),
                            ),
                            egui::Color32::WHITE,
                        );
                    }
                    let current_color = parse_hex_color(&self.video_text_color)
                        .unwrap_or(egui::Color32::WHITE);
                    let faded = current_color.gamma_multiply(0.65);
                    let card_rect = egui::Rect::from_min_max(
                        egui::pos2(preview_rect.left() + 48.0, preview_rect.bottom() - 250.0),
                        egui::pos2(preview_rect.right() - 48.0, preview_rect.bottom() - 40.0),
                    );
                    painter.rect_filled(
                        card_rect,
                        18.0,
                        egui::Color32::from_rgba_unmultiplied(10, 10, 10, self.video_card_opacity),
                    );
                    if !self.video_corner_tag.trim().is_empty() {
                        let tag_size =
                            estimate_tag_square_size(self.video_corner_tag.trim(), self.video_tag_font_size)
                                as f32;
                        let tag_min = match self.video_tag_position {
                            VideoTagPosition::TopLeft => {
                                egui::pos2(preview_rect.left(), preview_rect.top())
                            }
                            VideoTagPosition::TopCenter => egui::pos2(
                                preview_rect.center().x - tag_size / 2.0,
                                preview_rect.top(),
                            ),
                        };
                        let tag_rect = egui::Rect::from_min_max(
                            tag_min,
                            egui::pos2(tag_min.x + tag_size, tag_min.y + tag_size),
                        );
                        if self.video_tag_background_enabled {
                            painter.rect_filled(
                                tag_rect,
                                0.0,
                                egui::Color32::from_rgba_unmultiplied(
                                    10,
                                    10,
                                    10,
                                    self.video_card_opacity,
                                ),
                            );
                        }
                        let tag_font = egui::FontId::new(
                            self.video_tag_font_size as f32,
                            video_preview_font_family(&self.video_font_name),
                        );
                        painter.text(
                            egui::pos2(tag_rect.left() + 14.0, tag_rect.top() + 14.0),
                            egui::Align2::LEFT_TOP,
                            self.video_corner_tag.trim(),
                            tag_font,
                            current_color,
                        );
                    }
                    let active_index = self.current_video_preview_line_index();
                    let elapsed_seconds = self.current_video_preview_seconds();
                    let active_progress =
                        if let Some(active) = self.video_timed_lines.get(active_index) {
                            ((elapsed_seconds - active.start_seconds)
                                / (active.end_seconds - active.start_seconds).max(0.1))
                                .clamp(0.0, 1.0) as f32
                        } else {
                            0.0
                        };
                    let preview_font_family = video_preview_font_family(&self.video_font_name);
                    let font_id_current = egui::FontId::new(
                        self.video_font_size as f32,
                        preview_font_family.clone(),
                    );
                    let _font_id_other = egui::FontId::new(
                        (self.video_font_size as f32 * 0.74).max(20.0),
                        preview_font_family,
                    );
                    let preview_max_chars = if self.video_resolution == VideoResolution::FullHd1080 {
                        42
                    } else {
                        34
                    };
                    let text_rect = egui::Rect::from_min_max(
                        egui::pos2(card_rect.left() + 28.0, card_rect.top() + 26.0),
                        egui::pos2(card_rect.right() - 28.0, card_rect.bottom() - 30.0),
                    );
                    let text_painter = painter.with_clip_rect(text_rect);
                    let wrap_width = text_rect.width().max(120.0);
                    if !self.video_timed_lines.is_empty() {
                        let current_line_height = (self.video_font_size as f32 * 1.16).round() as u32;
                        let previous_line_height =
                            ((self.video_font_size as f32 * 0.74).max(20.0) * 1.12).round() as u32;
                        let preview_gap = (self.video_font_size as f32 * 0.42).round() as u32;
                        let preview_rendered = self
                            .video_timed_lines
                            .iter()
                            .map(|line| {
                                let wrapped = wrap_preview_text(
                                    &display_subtitle_text(&line.speaker, &line.text),
                                    preview_max_chars,
                                );
                                let line_count = wrapped.lines().count().max(1) as u32;
                                SubtitleRenderLine {
                                    wrapped,
                                    current_height: line_count.saturating_mul(current_line_height),
                                    previous_height: line_count.saturating_mul(previous_line_height),
                                }
                            })
                            .collect::<Vec<_>>();
                        let end_state =
                            build_subtitle_state_layout(
                                active_index,
                                &preview_rendered,
                                text_rect.top().round() as u32,
                                text_rect.bottom().round() as u32,
                                preview_gap,
                            );
                        let active_line = &self.video_timed_lines[active_index];
                        let transition_seconds = if active_index == 0 {
                            0.0
                        } else {
                            ((active_line.end_seconds - active_line.start_seconds).max(0.18) * 0.18)
                                .clamp(0.10, 0.22)
                        };
                        let transition_progress = if active_index == 0 || transition_seconds <= 0.0 {
                            1.0
                        } else {
                            ((elapsed_seconds - active_line.start_seconds) / transition_seconds)
                                .clamp(0.0, 1.0) as f32
                        };
                        let start_state = if active_index == 0 || transition_progress >= 1.0 {
                            end_state.clone()
                        } else {
                            build_subtitle_state_layout(
                                active_index - 1,
                                &preview_rendered,
                                text_rect.top().round() as u32,
                                text_rect.bottom().round() as u32,
                                preview_gap,
                            )
                        };

                        let mut union_indices = std::collections::BTreeSet::new();
                        for item in &start_state {
                            union_indices.insert(item.index);
                        }
                        for item in &end_state {
                            union_indices.insert(item.index);
                        }

                        let clip_top = text_rect.top();
                        let clip_bottom = text_rect.bottom();
                        for line_index in union_indices {
                            let start_item = start_state.iter().find(|item| item.index == line_index);
                            let end_item = end_state.iter().find(|item| item.index == line_index);
                            let start_bottom = if let Some(item) = start_item {
                                item.bottom_y as f32
                            } else {
                                clip_bottom + preview_rendered[line_index].current_height as f32 + preview_gap as f32
                            };
                            let end_bottom = if let Some(item) = end_item {
                                item.bottom_y as f32
                            } else {
                                clip_top - preview_rendered[line_index].previous_height as f32 - preview_gap as f32
                            };
                            let current_bottom =
                                start_bottom + (end_bottom - start_bottom) * transition_progress;
                            let start_is_current =
                                start_item.is_some_and(|item| item.style_name == "Current");
                            let end_is_current =
                                end_item.is_some_and(|item| item.style_name == "Current");
                            let color = lerp_color32(
                                if start_is_current { current_color } else { faded },
                                if end_is_current { current_color } else { faded },
                                transition_progress,
                            );
                            let font_id = egui::FontId::new(
                                lerp_f32(
                                    if start_is_current {
                                        self.video_font_size as f32
                                    } else {
                                        (self.video_font_size as f32 * 0.74).max(20.0)
                                    },
                                    if end_is_current {
                                        self.video_font_size as f32
                                    } else {
                                        (self.video_font_size as f32 * 0.74).max(20.0)
                                    },
                                    transition_progress,
                                ),
                                font_id_current.family.clone(),
                            );
                            let is_current = line_index == active_index && end_is_current;
                            let galley = if is_current {
                                let job = build_preview_layout_job(
                                    &self.video_timed_lines[line_index],
                                    self.current_video_preview_word_index(line_index),
                                    font_id,
                                    color,
                                    egui::Color32::from_rgb(255, 217, 102),
                                    wrap_width,
                                    true,
                                );
                                text_painter.layout_job(job)
                            } else {
                                let job = build_preview_layout_job(
                                    &self.video_timed_lines[line_index],
                                    None,
                                    font_id,
                                    color,
                                    color,
                                    wrap_width,
                                    true,
                                );
                                text_painter.layout_job(job)
                            };
                            let size = galley.size();
                            let pos = egui::pos2(
                                text_rect.center().x - size.x * 0.5,
                                current_bottom - size.y,
                            );
                            if pos.y > clip_bottom || pos.y + size.y < clip_top {
                                continue;
                            }
                            if is_current {
                                let accent_rect = egui::Rect::from_min_max(
                                    egui::pos2(text_rect.left(), pos.y + 4.0),
                                    egui::pos2(
                                        text_rect.left() + 5.0,
                                        (pos.y + size.y - 4.0).max(pos.y + 10.0),
                                    ),
                                );
                                text_painter.rect_filled(accent_rect, 4.0, current_color);
                            }
                            text_painter.galley(pos, galley, color);
                        }
                    } else {
                        let fallback = "No subtitles available for preview.".to_string();
                        let galley = text_painter.layout(
                            fallback,
                            font_id_current.clone(),
                            current_color,
                            wrap_width,
                        );
                        let size = galley.size();
                        let pos = egui::pos2(
                            text_rect.center().x - size.x * 0.5,
                            text_rect.bottom() - size.y - 8.0,
                        );
                        text_painter.galley(pos, galley, current_color);
                    }
                    if !self.video_timed_lines.is_empty() {
                        let progress_rect = egui::Rect::from_min_max(
                            egui::pos2(card_rect.left() + 24.0, card_rect.bottom() - 18.0),
                            egui::pos2(card_rect.right() - 24.0, card_rect.bottom() - 10.0),
                        );
                        painter.rect_filled(progress_rect, 8.0, egui::Color32::from_rgba_unmultiplied(255, 255, 255, 40));
                        let fill_rect = egui::Rect::from_min_max(
                            progress_rect.min,
                            egui::pos2(
                                progress_rect.left() + progress_rect.width() * active_progress,
                                progress_rect.bottom(),
                            ),
                        );
                        painter.rect_filled(fill_rect, 8.0, current_color);
                    }

                    ui.add_space(8.0);
                    if self.is_exporting_video || !self.video_progress_label.is_empty() {
                        ui.add(
                            egui::ProgressBar::new(self.video_progress_fraction.clamp(0.0, 1.0))
                                .desired_width(ui.available_width())
                                .text(self.video_progress_label.clone()),
                        );
                    }
                    ui.horizontal_wrapped(|ui| {
                        if self.is_exporting_video
                            && ui.button("Stop video job").clicked()
                        {
                            self.stop_video_job();
                        }
                        if ui
                            .add_enabled(
                                !self.is_exporting_video && self.can_export_video(),
                                egui::Button::new(if self.is_exporting_video {
                                        "Creating SRT..."
                                } else {
                                        "Create SRT"
                                }),
                            )
                            .clicked()
                        {
                            self.start_generate_video_srt();
                        }
                        if !self.video_srt_path.trim().is_empty()
                            && ui.button("Open SRT").clicked()
                        {
                            match open_in_browser(&self.video_srt_path) {
                                Ok(()) => self.video_status = "Opened SRT file.".to_string(),
                                Err(err) => {
                                    self.video_status = format!("Could not open SRT: {}", err)
                                }
                            }
                        }
                        if ui
                            .add_enabled(
                                !self.video_timed_lines.is_empty() && !self.is_playing,
                                egui::Button::new("Play preview"),
                            )
                            .clicked()
                        {
                            self.start_video_preview_playback();
                        }
                        if ui
                            .add_enabled(self.is_playing, egui::Button::new("Stop preview"))
                            .clicked()
                        {
                            self.stop_audio();
                            self.video_preview_started_at = None;
                            self.video_status = "Stopped video preview.".to_string();
                        }
                        if self.can_export_video() {
                            if ui
                                .add_enabled(
                                    !self.is_exporting_video && !self.video_timed_lines.is_empty(),
                                    egui::Button::new(if self.is_exporting_video {
                                        "Exporting preview..."
                                    } else {
                                        "Export preview clip"
                                    }),
                                )
                                .clicked()
                            {
                                let _ = self.persist_state_to_disk();
                                self.start_video_preview_export();
                            }
                        }
                        if self.can_export_video() {
                            if ui
                                .add_enabled(
                                    !self.is_exporting_video && !self.video_timed_lines.is_empty(),
                                    egui::Button::new(if self.is_exporting_video {
                                        "Exporting video..."
                                    } else {
                                        "Start video export"
                                    }),
                                )
                                .clicked()
                            {
                                let _ = self.persist_state_to_disk();
                                self.start_video_export();
                            }
                        } else {
                            ui.label(format!(
                                "Video export is locked until every parsed line has speech cache. Missing: {}",
                                self.missing_cached_book_line_indices().len()
                            ));
                        }
                        if !self.last_export_dir.trim().is_empty()
                            && ui.button("Open export folder").clicked()
                        {
                            match open_path_in_explorer(Path::new(&self.last_export_dir)) {
                                Ok(()) => self.video_status = "Opened video export folder.".to_string(),
                                Err(err) => {
                                    self.video_status =
                                        format!("Could not open video export folder: {}", err)
                                }
                            }
                        }
                        if !self.last_export_dir.trim().is_empty()
                            && ui.button("Open preview").clicked()
                        {
                            let preview_dir = project_video_preview_dir(Path::new(&self.last_export_dir));
                            match open_path_in_explorer(&preview_dir) {
                                Ok(()) => self.video_status = "Opened preview folder.".to_string(),
                                Err(err) => {
                                    self.video_status =
                                        format!("Could not open preview folder: {}", err)
                                }
                            }
                        }
                        if !self.last_export_dir.trim().is_empty()
                            && ui.button("Open video folder").clicked()
                        {
                            let final_dir = project_video_final_dir(Path::new(&self.last_export_dir));
                            match open_path_in_explorer(&final_dir) {
                                Ok(()) => self.video_status = "Opened video folder.".to_string(),
                                Err(err) => {
                                    self.video_status =
                                        format!("Could not open video folder: {}", err)
                                }
                            }
                        }
                        if !self.last_video_export_path.trim().is_empty()
                            && ui.button("Open video").clicked()
                        {
                            match open_in_browser(&self.last_video_export_path) {
                                Ok(()) => self.video_status = "Opened video file.".to_string(),
                                Err(err) => {
                                    self.video_status =
                                        format!("Could not open video file: {}", err)
                                }
                            }
                        }
                    });
                });
        }

        text_edits.sort_by_key(|(index, _)| *index);
        text_edits.dedup_by(|a, b| a.0 == b.0);
        for (index, text) in text_edits {
            self.set_book_line_text(index, &text);
        }

        speaker_edits.sort_by_key(|(index, _)| *index);
        speaker_edits.dedup_by(|a, b| a.0 == b.0);
        for (index, speaker) in speaker_edits {
            self.set_book_line_speaker(index, &speaker);
        }

        if let Some(voice) = preview_clicked {
            self.start_speak_preview(Some(voice));
        }
        if let Some(index) = line_preview_clicked {
            self.start_book_line_preview(index);
        }
        if let Some(index) = cached_preview_clicked {
            self.play_cached_speech(index);
        }
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        let _ = self.persist_state_to_disk();
    }
}

fn run_tts_job(job: TtsJob, cancel_flag: Arc<AtomicBool>) -> Result<Vec<i16>> {
    validate_api_key(&job.api_key)?;
    let mut socket = connect_tts_websocket(&job.api_key)?;
    send_tts_setup(
        &mut socket,
        &job.voice_name,
        job.speed,
        Some(job.style_instruction.as_str()),
    )?;
    wait_setup_complete(&mut socket, &cancel_flag)?;
    send_tts_text(&mut socket, &prepare_gemini_live_text(&job.text))?;
    read_audio_stream(&mut socket, &cancel_flag)
}

fn run_tts_job_with_chunking(job: &TtsJob) -> Result<Vec<i16>> {
    let segments = split_tts_segments(&job.text);
    let mut all_samples = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        let mut segment_job = job.clone();
        segment_job.text = segment.clone();
        let mut last_error: Option<anyhow::Error> = None;
        let mut samples_result: Option<Vec<i16>> = None;

        for _ in 0..=GEMINI_AUDIO_RETRIES {
            match run_tts_job(segment_job.clone(), Arc::new(AtomicBool::new(false))) {
                Ok(samples) => {
                    samples_result = Some(samples);
                    break;
                }
                Err(err) => {
                    last_error = Some(err);
                    thread::sleep(Duration::from_millis(350));
                }
            }
        }

        let samples = if let Some(samples) = samples_result {
            samples
        } else {
            return Err(last_error.unwrap_or_else(|| anyhow!("Gemini khong tra ve audio.")));
        };
        append_samples_with_pause(&mut all_samples, &samples, index > 0, 140);
    }
    Ok(all_samples)
}

fn run_tts_job_base_resilient(job: &TtsJob) -> Result<Vec<i16>> {
    match run_tts_job_with_chunking(job) {
        Ok(samples) => Ok(samples),
        Err(err) => {
            let message = err.to_string();
            if should_retry_short_gemini_line(&job.text, &message) {
                synthesize_short_gemini_line(job).map_err(|fallback_err| {
                    anyhow!(
                        "{} | short-line fallback that bai: {}",
                        message,
                        fallback_err
                    )
                })
            } else if contains_dialogue_quotes(&job.text)
                && message.to_lowercase().contains("khong tra ve audio")
            {
                synthesize_quote_aware_gemini_line(job).map_err(|fallback_err| {
                    anyhow!(
                        "{} | quote-aware fallback that bai: {}",
                        message,
                        fallback_err
                    )
                })
            } else {
                Err(err)
            }
        }
    }
}

fn run_tts_job_resilient(job: &TtsJob) -> Result<Vec<i16>> {
    run_tts_job_base_resilient(job)
}

fn should_retry_short_gemini_line(text: &str, error_message: &str) -> bool {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let short_text = normalized.chars().count() <= GEMINI_SHORT_LINE_THRESHOLD
        || normalized.split_whitespace().count() <= 4;
    short_text && error_message.to_lowercase().contains("khong tra ve audio")
}

fn should_retry_split_gemini_line(text: &str, error_message: &str) -> bool {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let is_long = normalized.chars().count() >= GEMINI_LONG_LINE_SPLIT_THRESHOLD;
    if !is_long {
        return false;
    }

    let lowered = error_message.to_lowercase();
    lowered.contains("http status: 429")
        || lowered.contains("khong tra ve audio")
        || lowered.contains("timeout")
        || lowered.contains("too many requests")
}

fn synthesize_split_gemini_line(job: &TtsJob) -> Result<Vec<i16>> {
    let parts = split_gemini_line_for_retry(&job.text);
    if parts.len() < 3 {
        return Err(anyhow!("Kh?ng tach duoc dong ??i de retry Gemini."));
    }
    let mut combined = Vec::new();
    for (index, part) in parts.iter().take(3).enumerate() {
        let mut part_job = job.clone();
        part_job.text = part.clone();
        let part_label = match index {
            0 => "part 1",
            1 => "part 2",
            _ => "part 3",
        };
        let samples = synthesize_gemini_part_with_sentence_fallback(&part_job, part_label, part)?;
        append_samples_with_pause(&mut combined, &samples, index > 0, 140);
    }
    Ok(combined)
}

fn synthesize_gemini_part_with_sentence_fallback(
    job: &TtsJob,
    part_label: &str,
    original_part: &str,
) -> Result<Vec<i16>> {
    match run_tts_job_base_resilient(job) {
        Ok(samples) => Ok(samples),
        Err(err) => {
            let sentences = split_text_into_sentences(original_part)
                .into_iter()
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>();
            if sentences.len() < 2 {
                return Err(anyhow!(
                    "{} failed: {} | excerpt: {}",
                    part_label,
                    err,
                    excerpt_for_error(original_part)
                ));
            }

            let mut combined = Vec::new();
            for (sentence_index, sentence) in sentences.iter().enumerate() {
                let mut sentence_job = job.clone();
                sentence_job.text = sentence.clone();
                let sentence_samples =
                    run_tts_job_base_resilient(&sentence_job).map_err(|sentence_err| {
                        anyhow!(
                            "{} sentence {} failed: {} | excerpt: {}",
                            part_label,
                            sentence_index + 1,
                            sentence_err,
                            excerpt_for_error(sentence)
                        )
                    })?;
                append_samples_with_pause(
                    &mut combined,
                    &sentence_samples,
                    sentence_index > 0,
                    140,
                );
            }
            Ok(combined)
        }
    }
}

fn synthesize_short_gemini_line(job: &TtsJob) -> Result<Vec<i16>> {
    let compact = job.text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        return Err(anyhow!("Kh?ng co text de fallback Gemini."));
    }

    let mut retry_job = job.clone();
    retry_job.text = if compact.ends_with(['.', '!', '?']) {
        compact.clone()
    } else {
        format!("{compact}.")
    };
    retry_job.style_instruction = if retry_job.style_instruction.trim().is_empty() {
        "Read every word completely. Do not omit or truncate the final word.".to_string()
    } else {
        format!(
            "{} {}",
            retry_job.style_instruction.trim(),
            "Read every word completely. Do not omit or truncate the final word."
        )
    };

    let mut last_error: Option<anyhow::Error> = None;
    for _ in 0..=GEMINI_AUDIO_RETRIES {
        match run_tts_job(retry_job.clone(), Arc::new(AtomicBool::new(false))) {
            Ok(samples) => return Ok(trim_trailing_silence(&samples)),
            Err(err) => {
                last_error = Some(err);
                thread::sleep(Duration::from_millis(350));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("Gemini short-line fallback khong tra ve audio.")))
}

fn normalize_gemini_tts_text(input: &str) -> String {
    input
        .replace(['\u{FFFD}', '\u{201C}'], "\"")
        .replace(['\u{FFFD}', '\u{2019}'], "'")
        .replace('\u{2014}', " - ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn prepare_gemini_live_text(input: &str) -> String {
    ensure_sentence_like_text(&normalize_gemini_tts_text(input)).to_lowercase()
}

fn excerpt_for_error(text: &str) -> String {
    let normalized = normalize_gemini_tts_text(text);
    let chars = normalized.chars().collect::<Vec<_>>();
    if chars.len() <= 180 {
        return normalized;
    }
    let head = chars[..180].iter().collect::<String>();
    format!("{}...", head.trim())
}

fn split_gemini_line_for_retry(text: &str) -> Vec<String> {
    let normalized = normalize_gemini_tts_text(text);
    let sentences = split_text_into_sentences(&normalized)
        .into_iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();

    let segments = split_tts_segments(&normalized);
    let base_parts = if sentences.len() >= 2 { sentences } else { segments };

    if base_parts.len() >= 3 {
        let total_chars = base_parts
            .iter()
            .map(|part| part.chars().count())
            .sum::<usize>();
        let target = (total_chars / 3).max(1);
        let mut first = Vec::new();
        let mut second = Vec::new();
        let mut third = Vec::new();
        let mut first_chars = 0usize;
        let mut second_chars = 0usize;

        for part in base_parts {
            if first_chars < target || first.is_empty() {
                first_chars += part.chars().count();
                first.push(part);
            } else if second_chars < target || second.is_empty() {
                second_chars += part.chars().count();
                second.push(part);
            } else {
                third.push(part);
            }
        }

        if !first.is_empty() && !second.is_empty() && !third.is_empty() {
            return vec![first.join(" "), second.join(" "), third.join(" ")];
        }
    }

    let word_groups = split_text_into_word_groups(&normalized, 20);
    if word_groups.len() >= 3 {
        let first_cut = word_groups.len() / 3;
        let second_cut = (word_groups.len() * 2) / 3;
        if first_cut > 0 && second_cut > first_cut && second_cut < word_groups.len() {
            return vec![
                word_groups[..first_cut].join(" "),
                word_groups[first_cut..second_cut].join(" "),
                word_groups[second_cut..].join(" "),
            ];
        }
    }

    let words = normalized.split_whitespace().collect::<Vec<_>>();
    if words.len() >= 9 {
        let first_cut = words.len() / 3;
        let second_cut = (words.len() * 2) / 3;
        if first_cut > 0 && second_cut > first_cut && second_cut < words.len() {
            return vec![
                words[..first_cut].join(" "),
                words[first_cut..second_cut].join(" "),
                words[second_cut..].join(" "),
            ];
        }
    }

    Vec::new()
}

fn split_text_into_word_groups(text: &str, words_per_group: usize) -> Vec<String> {
    let words = text
        .split_whitespace()
        .map(str::trim)
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    if words.len() <= words_per_group {
        return Vec::new();
    }

    words
        .chunks(words_per_group.max(4))
        .map(|chunk| chunk.join(" "))
        .collect()
}

fn chunk_text_parts(parts: &[String], max_chars: usize) -> Vec<String> {
    if parts.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut current = String::new();

    for part in parts {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.chars().count() > max_chars {
            let nested = split_tts_segments(trimmed)
                .into_iter()
                .map(|segment| segment.trim().to_string())
                .filter(|segment| !segment.is_empty())
                .collect::<Vec<_>>();
            if nested.len() > 1 {
                if !current.trim().is_empty() {
                    chunks.push(current.trim().to_string());
                    current.clear();
                }
                chunks.extend(chunk_text_parts(&nested, max_chars));
                continue;
            }
        }

        let candidate = if current.is_empty() {
            trimmed.to_string()
        } else {
            format!("{} {}", current.trim(), trimmed)
        };

        if !current.is_empty() && candidate.chars().count() > max_chars {
            chunks.push(current.trim().to_string());
            current = trimmed.to_string();
        } else {
            current = candidate;
        }
    }

    if !current.trim().is_empty() {
        chunks.push(current.trim().to_string());
    }

    chunks
}

fn contains_dialogue_quotes(text: &str) -> bool {
    normalize_gemini_tts_text(text).matches('"').count() >= 2
}

fn synthesize_quote_aware_gemini_line(job: &TtsJob) -> Result<Vec<i16>> {
    let normalized = normalize_gemini_tts_text(&job.text);
    let Some((narration, dialogue)) = split_narrator_quote_from_text(&normalized) else {
        return Err(anyhow!("Kh?ng tach duoc narrator co quote de fallback Gemini."));
    };

    let mut segments = Vec::new();
    if !narration.trim().is_empty() {
        segments.push(compact_narration_text(&narration));
    }
    if !dialogue.trim().is_empty() {
        segments.push(ensure_sentence_like_text(dialogue.trim()));
    }
    if segments.is_empty() {
        return Err(anyhow!("Kh?ng con segment nao sau khi tach quote."));
    }

    let mut all_samples = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        let mut segment_job = job.clone();
        segment_job.text = segment.clone();
        segment_job.style_instruction = if segment_job.style_instruction.trim().is_empty() {
            "Read every word completely. Do not omit any quoted words.".to_string()
        } else {
            format!(
                "{} {}",
                segment_job.style_instruction.trim(),
                "Read every word completely. Do not omit any quoted words."
            )
        };

        let mut last_error: Option<anyhow::Error> = None;
        let mut samples_result: Option<Vec<i16>> = None;
        for _ in 0..=GEMINI_AUDIO_RETRIES {
            match run_tts_job(segment_job.clone(), Arc::new(AtomicBool::new(false))) {
                Ok(samples) => {
                    samples_result = Some(samples);
                    break;
                }
                Err(err) => {
                    last_error = Some(err);
                    thread::sleep(Duration::from_millis(300));
                }
            }
        }

        let samples = if let Some(samples) = samples_result {
            samples
        } else {
            return Err(last_error.unwrap_or_else(|| anyhow!("Gemini quote-aware fallback khong tra ve audio.")));
        };

        append_samples_with_pause(
            &mut all_samples,
            &trim_trailing_silence(&samples),
            index > 0,
            120,
        );
    }

    Ok(all_samples)
}

fn trim_trailing_silence(samples: &[i16]) -> Vec<i16> {
    let threshold = 350i32;
    let mut end = samples.len();
    while end > 0 && i32::from(samples[end - 1]).abs() <= threshold {
        end -= 1;
    }
    samples[..end].to_vec()
}

fn analyze_voice_with_gemini(job: &AnalysisJob) -> Result<VoiceAnalysis> {
    validate_api_key(&job.api_key)?;

    let mime_type = infer_audio_mime_type(&job.audio_path)
        .ok_or_else(|| anyhow!("Dinh ?ang audio khong duoc ho tro."))?;
    let bytes = fs::read(&job.audio_path)
        .with_context(|| format!("Kh?ng doc duoc file {}", job.audio_path.display()))?;

    if bytes.len() > MAX_INLINE_AUDIO_BYTES {
        return Err(anyhow!(
            "File qua lon de gui inline. Hay dung clip duoi {} MB.",
            MAX_INLINE_AUDIO_BYTES / (1024 * 1024)
        ));
    }

    let payload = serde_json::json!({
        "contents": [{
            "parts": [
                {
                    "inlineData": {
                        "mimeType": mime_type,
                        "data": general_purpose::STANDARD.encode(bytes)
                    }
                },
                {
                    "text": "Analyze this voice sample for speech-style transfer only. Do not identify the speaker and do not claim biometric cloning. Return valid JSON with exactly these keys: transcript, style_summary, tts_prompt. transcript: best-effort transcript of what is said. style_summary: Vietnamese explanation of pace, rhythm, pitch, resonance, breathiness, emotion, articulation, pauses, accent cues, intensity, and delivery. tts_prompt: Vietnamese prompt optimized for a TTS model to approximate the speaking style and performance of this sample without naming the person or claiming exact identity mimicry."
                }
            ]
        }],
        "generationConfig": {
            "responseMimeType": "application/json"
        }
    });

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        ANALYSIS_MODEL, job.api_key
    );

    let response = ureq::post(&url)
        .content_type("application/json")
        .send(&payload.to_string())
        .map_err(|err| anyhow!("Kh?ng goi duoc Gemini phan tich audio: {}", err))?;

    let body = response
        .into_body()
        .read_to_string()
        .map_err(|err| anyhow!("Kh?ng doc duoc phan hoi Gemini: {}", err))?;
    let json: Value =
        serde_json::from_str(&body).map_err(|err| anyhow!("JSON Gemini khong hop le: {}", err))?;

    let text = extract_text_from_generate_content(&json)
        .ok_or_else(|| anyhow!("Gemini khong tra noi dung text cho phan tich audio."))?;
    let analysis_json: Value = serde_json::from_str(&text)
        .map_err(|err| anyhow!("Gemini tra JSON phan tich khong hop le: {}", err))?;

    Ok(VoiceAnalysis {
        transcript: analysis_json
            .get("transcript")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        style_summary: analysis_json
            .get("style_summary")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        tts_prompt: analysis_json
            .get("tts_prompt")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    })
}

fn extract_text_from_generate_content(json: &Value) -> Option<String> {
    let candidates = json.get("candidates")?.as_array()?;
    let first = candidates.first()?;
    let parts = first.get("content")?.get("parts")?.as_array()?;
    let text = parts.first()?.get("text")?.as_str()?;
    Some(text.to_string())
}

fn infer_audio_mime_type(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_string_lossy().to_ascii_lowercase();
    match extension.as_str() {
        "mp3" => Some("audio/mpeg"),
        "wav" => Some("audio/wav"),
        "m4a" => Some("audio/mp4"),
        "mp4" => Some("audio/mp4"),
        "aac" => Some("audio/aac"),
        "flac" => Some("audio/flac"),
        "ogg" => Some("audio/ogg"),
        "webm" => Some("audio/webm"),
        _ => None,
    }
}

fn is_supported_image_file(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase()),
        Some(ext) if matches!(ext.as_str(), "png" | "jpg" | "jpeg" | "bmp" | "gif" | "webp")
    )
}

fn is_supported_srt_file(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase()),
        Some(ext) if ext == "srt"
    )
}

fn load_color_image_from_path(path: &Path) -> Result<egui::ColorImage> {
    let image = ImageReader::open(path)
        .with_context(|| format!("Kh?ng mo duoc anh {}", path.display()))?
        .decode()
        .with_context(|| format!("Kh?ng decode duoc anh {}", path.display()))?
        .to_rgba8();
    let size = [image.width() as usize, image.height() as usize];
    let pixels = image.into_raw();
    Ok(egui::ColorImage::from_rgba_unmultiplied(size, &pixels))
}

fn connect_tts_websocket(api_key: &str) -> Result<WebSocket<TlsStream<TcpStream>>> {
    let ws_url = format!(
        "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={api_key}"
    );

    let parsed = url::Url::parse(&ws_url).context("WebSocket URL khong hop le")?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("URL kh?ng c? host"))?;
    let addr = format!("{host}:443")
        .to_socket_addrs()
        .context("Kh?ng resolve duoc hostname Gemini")?
        .next()
        .ok_or_else(|| anyhow!("Kh?ng tim thay dia chi host"))?;

    let tcp_stream = TcpStream::connect_timeout(&addr, Duration::from_secs(10))
        .context("Kh?ng ket noi duoc den Gemini")?;
    tcp_stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .context("Kh?ng set duoc read timeout")?;
    tcp_stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .context("Kh?ng set duoc write timeout")?;
    tcp_stream
        .set_nodelay(true)
        .context("Kh?ng set duoc TCP_NODELAY")?;

    let connector = TlsConnector::new().context("Kh?ng tao duoc TLS connector")?;
    let tls_stream = connector
        .connect(host, tcp_stream)
        .context("TLS handshake that bai")?;

    let (socket, _) = client(&ws_url, tls_stream).context("WebSocket handshake that bai")?;
    Ok(socket)
}

fn validate_api_key(api_key: &str) -> Result<()> {
    let url = format!("https://generativelanguage.googleapis.com/v1beta/models?key={api_key}");

    match ureq::get(&url).call() {
        Ok(response) if response.status().is_success() => Ok(()),
        Ok(response) => Err(anyhow!(
            "Gemini tu choi API key voi HTTP {}.",
            response.status().as_u16()
        )),
        Err(err) => Err(anyhow!(
            "Gemini API key khong hop le hoac bi tu choi: {}",
            err
        )),
    }
}

fn send_tts_setup(
    socket: &mut WebSocket<TlsStream<TcpStream>>,
    voice_name: &str,
    speed: SpeechSpeed,
    custom_instructions: Option<&str>,
) -> Result<()> {
    let mut system_text = String::from(
        "You are a text-to-speech reader. Read exactly the provided text, word by word, without adding or removing content. ",
    );

    match speed {
        SpeechSpeed::Slow => system_text.push_str("Speak slowly and clearly. "),
        SpeechSpeed::Fast => system_text.push_str("Speak quickly but still clear. "),
        SpeechSpeed::Normal => system_text.push_str("Speak naturally and clearly. "),
    }

    if let Some(extra) = custom_instructions {
        let trimmed = extra.trim();
        if !trimmed.is_empty() {
            system_text.push_str("Additional speaking style instructions: ");
            system_text.push_str(trimmed);
            system_text.push(' ');
        }
    }
    system_text.push_str("Start reading immediately.");

    let setup = serde_json::json!({
        "setup": {
            "model": format!("models/{TTS_MODEL}"),
            "generationConfig": {
                "responseModalities": ["AUDIO"],
                "speechConfig": {
                    "voiceConfig": {
                        "prebuiltVoiceConfig": {
                            "voiceName": voice_name
                        }
                    }
                },
                "thinkingConfig": {
                    "thinkingBudget": 0
                }
            },
            "systemInstruction": {
                "parts": [{
                    "text": system_text
                }]
            }
        }
    });

    socket
        .write(Message::Text(setup.to_string().into()))
        .context("Gui setup toi Gemini that bai")?;
    socket.flush().context("Flush setup that bai")?;
    Ok(())
}

fn wait_setup_complete(
    socket: &mut WebSocket<TlsStream<TcpStream>>,
    cancel_flag: &AtomicBool,
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(12) {
        if cancel_flag.load(Ordering::SeqCst) {
            return Err(anyhow!("Da huy request."));
        }

        if let Some(text_msg) = read_json_text(socket)? {
            if text_msg.contains("setupComplete") {
                return Ok(());
            }
            if let Some(err) = extract_server_error(&text_msg) {
                return Err(anyhow!(err));
            }
        }
    }
    Err(anyhow!("Setup Gemini Live bi timeout."))
}

fn send_tts_text(socket: &mut WebSocket<TlsStream<TcpStream>>, text: &str) -> Result<()> {
    let prompt = format!("[READ ALOUD VERBATIM - START NOW]\n\n{text}");
    let msg = serde_json::json!({
        "clientContent": {
            "turns": [{
                "role": "user",
                "parts": [{
                    "text": prompt
                }]
            }],
            "turnComplete": true
        }
    });

    socket
        .write(Message::Text(msg.to_string().into()))
        .context("Gui text toi Gemini that bai")?;
    socket.flush().context("Flush text that bai")?;
    Ok(())
}

fn read_audio_stream(
    socket: &mut WebSocket<TlsStream<TcpStream>>,
    cancel_flag: &AtomicBool,
) -> Result<Vec<i16>> {
    let mut all_samples = Vec::new();
    let started_at = Instant::now();

    loop {
        if cancel_flag.load(Ordering::SeqCst) {
            return Err(anyhow!("Da huy request."));
        }

        if started_at.elapsed() > Duration::from_secs(GEMINI_TURN_TIMEOUT_SECS) {
            return Err(anyhow!(
                "Gemini Live bi timeout sau {} giay.",
                GEMINI_TURN_TIMEOUT_SECS
            ));
        }

        let maybe_msg = read_json_text(socket)?;
        let Some(msg_text) = maybe_msg else {
            continue;
        };

        if let Some(err) = extract_server_error(&msg_text) {
            return Err(anyhow!(err));
        }

        if let Some(chunk_bytes) = parse_audio_data(&msg_text) {
            for chunk in chunk_bytes.chunks_exact(2) {
                let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
                all_samples.push(sample);
            }
        }

        if is_turn_complete(&msg_text) {
            break;
        }
    }

    if all_samples.is_empty() {
        return Err(anyhow!("Gemini khong tra ve audio."));
    }
    Ok(all_samples)
}

fn read_json_text(socket: &mut WebSocket<TlsStream<TcpStream>>) -> Result<Option<String>> {
    match socket.read().context("Loi doc websocket")? {
        Message::Text(text) => Ok(Some(text.to_string())),
        Message::Binary(bin) => Ok(String::from_utf8(bin.to_vec()).ok()),
        Message::Ping(payload) => {
            socket
                .send(Message::Pong(payload))
                .context("Gui Pong that bai")?;
            Ok(None)
        }
        Message::Pong(_) => Ok(None),
        Message::Close(frame) => match frame {
            Some(frame) => Err(anyhow!(
                "WebSocket ?? dong: code={} reason={}",
                frame.code,
                frame.reason
            )),
            None => Err(anyhow!("WebSocket ?? dong ma kh?ng c? ly do.")),
        },
        Message::Frame(_) => Ok(None),
    }
}

fn parse_audio_data(message: &str) -> Option<Vec<u8>> {
    let json: Value = serde_json::from_str(message).ok()?;
    let parts = json
        .get("serverContent")?
        .get("modelTurn")?
        .get("parts")?
        .as_array()?;

    for part in parts {
        let inline_data = part.get("inlineData")?;
        let encoded = inline_data.get("data")?.as_str()?;
        if let Ok(bytes) = general_purpose::STANDARD.decode(encoded) {
            return Some(bytes);
        }
    }
    None
}

fn is_turn_complete(message: &str) -> bool {
    let Ok(json) = serde_json::from_str::<Value>(message) else {
        return false;
    };
    let Some(server_content) = json.get("serverContent") else {
        return false;
    };

    server_content
        .get("turnComplete")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || server_content
            .get("generationComplete")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

fn extract_server_error(message: &str) -> Option<String> {
    let json: Value = serde_json::from_str(message).ok()?;
    let error_obj = json.get("error")?;
    if let Some(msg) = error_obj.get("message").and_then(Value::as_str) {
        return Some(format!("Gemini error: {msg}"));
    }
    Some(format!("Gemini error: {error_obj}"))
}

fn format_book_text_with_gemini(job: &BookFormatJob) -> Result<String> {
    let formatted = format_book_paragraph_with_gemini(&job.api_key, &job.source_text)?;
    let normalized = normalize_book_output_aliases(formatted.trim());
    let repaired = repair_book_dialogue_blocks(&normalized);
    Ok(sanitize_formatted_book_lines(&repaired))
}

fn format_book_paragraph_with_gemini(api_key: &str, paragraph: &str) -> Result<String> {
    let prompt = build_book_format_prompt(paragraph);
    let payload = serde_json::json!({
        "contents": [{
            "parts": [{
                "text": prompt
            }]
        }]
    });

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        ANALYSIS_MODEL, api_key
    );

    let payload_text = payload.to_string();
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 0..=BOOK_GEMINI_RETRIES {
        match ureq::post(&url)
            .content_type("application/json")
            .send(&payload_text)
        {
            Ok(response) => {
                let body = response
                    .into_body()
                    .read_to_string()
                    .map_err(|err| anyhow!("Kh?ng doc duoc phan hoi Gemini: {}", err))?;
                let json: Value = serde_json::from_str(&body)
                    .map_err(|err| anyhow!("JSON Gemini khong hop le: {}", err))?;

                return extract_candidate_text(&json)
                    .ok_or_else(|| anyhow!("Gemini khong tra ve text cho book formatter."));
            }
            Err(err) => {
                let message = err.to_string();
                let is_retryable = message.contains("429")
                    || message.contains("503")
                    || message.to_ascii_lowercase().contains("rate")
                    || message.to_ascii_lowercase().contains("quota");
                last_error = Some(if message.contains("429") {
                    anyhow!("Gemini ?ang b? rate limit ho?c h?t quota t?m th?i: {}", message)
                } else {
                    anyhow!("Kh?ng goi duoc Gemini xu ly book text: {}", message)
                });

                if is_retryable && attempt < BOOK_GEMINI_RETRIES {
                    let backoff_ms = 10_000_u64.saturating_mul((attempt + 1) as u64);
                    thread::sleep(Duration::from_millis(backoff_ms));
                    continue;
                }
                break;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("Kh?ng goi duoc Gemini xu ly book text.")))
}

fn build_book_format_prompt(source_text: &str) -> String {
    format!(
        "You are converting fiction prose into audiobook speaker blocks. Keep the original wording intact. Do not summarize. Do not censor profanity. Do not omit any sentence, action beat, narration beat, dialogue tag, or short narration line. This is critical: every sentence from the source must appear in the output exactly once in order. Never shorten names inside narration. Never rewrite narration. Remove only quote punctuation when turning dialogue into speaker lines. Use this exact style: narrator: ... or CharacterName: ... Put each block on its own paragraph. If the speaker is clear, use that speaker name exactly as written in the source. If the text is narration, use narrator. Keep consecutive narration sentences in the same narrator block unless dialogue interrupts them. Do not split ordinary narration into multiple narrator blocks just because there are multiple sentences. If a sentence contains both narration and quoted dialogue, split them into separate blocks in order: narrator for the narration words, then CharacterName for the quoted words. Never leave quoted dialogue inside a narrator line when the speaker can be inferred. Never output bare quotes as their own block. Never output partial quote fragments. Do not rename speakers yourself; the app will normalize speaker labels after you respond. Return plain text only.\n\nExample input:\n\"You're an awesome player to watch,\" Shane Hollander said.\n\n\"I know.\" If Shane Hollander was expecting Ilya Rozanov to return the compliment, he was going to be waiting a long damn time.\n\nWhen Ilya Rozanov didn't say anything else, Shane Hollander changed the subject. \"Are your parents here with you?\"\n\n\"No.\"\n\nIlya Rozanov settled back against the wall and lit his cigarette. This fucking country. Bad enough he couldn't smoke indoors anywhere-he needed to go sit in the fucking snow while he did it?\n\nExample output:\nShane Hollander: You're an awesome player to watch\n\nnarrator: Shane Hollander said.\n\nIlya Rozanov: I know.\n\nnarrator: If Shane Hollander was expecting Ilya Rozanov to return the compliment, he was going to be waiting a long damn time.\n\nnarrator: When Ilya Rozanov didn't say anything else, Shane Hollander changed the subject.\n\nShane Hollander: Are your parents here with you?\n\nIlya Rozanov: No.\n\nnarrator: Ilya Rozanov settled back against the wall and lit his cigarette. This fucking country. Bad enough he couldn't smoke indoors anywhere-he needed to go sit in the fucking snow while he did it?\n\nNow convert this text:\n{}",
        source_text
    )
}

fn split_book_paragraphs(input: &str) -> Vec<String> {
    input
        .replace("\r\n", "\n")
        .split("\n\n")
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect()
}

fn paragraph_needs_gemini(paragraph: &str) -> bool {
    let normalized = paragraph.replace(['\u{201C}', '\u{201D}'], "\"");
    normalized.contains('"')
}

fn extract_candidate_text(json: &Value) -> Option<String> {
    let candidates = json.get("candidates")?.as_array()?;
    let first = candidates.first()?;
    let parts = first.get("content")?.get("parts")?.as_array()?;
    let mut texts = Vec::new();
    for part in parts {
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                texts.push(trimmed.to_string());
            }
        }
    }
    if texts.is_empty() {
        None
    } else {
        Some(texts.join("\n"))
    }
}

fn analyze_characters_with_gemini(job: &CharacterAnalysisJob) -> Result<Vec<CharacterRecord>> {
    validate_api_key(&job.api_key)?;

    let payload = serde_json::json!({
        "contents": [{
            "parts": [{
                "text": format!(
                    "Extract the important recurring characters from this fiction text. Return JSON only with this shape: {{\"characters\":[{{\"name\":\"...\",\"description\":\"short English voice description\"}}]}}. Do not include narrator. Speaker alias rule: if the character is Hollander, return the name Shane instead. If the character is Rozanov, return the name Ilya instead. Make each description short, in English, and useful for voice preview. Text:\n{}",
                    job.source_text
                )
            }]
        }],
        "generationConfig": {
            "responseMimeType": "application/json"
        }
    });

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        ANALYSIS_MODEL, job.api_key
    );

    let response = ureq::post(&url)
        .content_type("application/json")
        .send(&payload.to_string())
        .map_err(|err| anyhow!("Kh?ng goi duoc Gemini phan tich nhan vat: {}", err))?;

    let body = response
        .into_body()
        .read_to_string()
        .map_err(|err| anyhow!("Kh?ng doc duoc phan hoi Gemini: {}", err))?;
    let json: Value =
        serde_json::from_str(&body).map_err(|err| anyhow!("JSON Gemini khong hop le: {}", err))?;

    let raw_text = extract_candidate_text(&json)
        .ok_or_else(|| anyhow!("Gemini khong tra ve ??nh sach nhan vat."))?;
    let parsed: Value = serde_json::from_str(&raw_text)
        .map_err(|err| anyhow!("JSON nhan vat khong hop le: {}", err))?;

    let mut characters = Vec::new();
    if let Some(items) = parsed.get("characters").and_then(Value::as_array) {
        for item in items {
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            if name.is_empty() {
                continue;
            }
            let description = item
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            characters.push(CharacterRecord {
                name,
                description,
                ref_text: String::new(),
                ref_audio_path: String::new(),
                tts_engine: "qwen".to_string(),
                gemini_voice: DEFAULT_VOICE.to_string(),
                gemini_style_prompt: String::new(),
                gemini_speed: SpeechSpeed::Normal,
                volume_percent: 100,
            });
        }
    }

    Ok(characters)
}

fn slugify_name(name: &str) -> String {
    let mut out = String::new();
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch.is_whitespace() || ch == '-' || ch == '_' {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "character".to_string()
    } else {
        trimmed
    }
}

fn copy_character_audio(name: &str, source: &Path) -> Result<PathBuf> {
    if !source.exists() {
        return Err(anyhow!("Kh?ng tim thay file audio {}", source.display()));
    }

    let slug = slugify_name(name);
    let ext = source
        .extension()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("wav");
    let dir = app_data_dir().join("characters").join(slug);
    fs::create_dir_all(&dir).with_context(|| format!("Kh?ng tao duoc {}", dir.display()))?;
    let target = dir.join(format!("reference.{}", ext));
    fs::copy(source, &target).with_context(|| {
        format!(
            "Kh?ng copy duoc audio {} -> {}",
            source.display(),
            target.display()
        )
    })?;
    Ok(target)
}

#[derive(Deserialize)]
struct QwenGenerateResponse {
    output_path: String,
    ref_text: String,
}

struct QwenChunkedResult {
    samples: Vec<i16>,
    ref_text: String,
}

fn parse_book_lines(input: &str) -> Vec<BookLine> {
    let mut merged: Vec<BookLine> = Vec::new();

    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((speaker, text)) = trimmed.split_once(':') else {
            continue;
        };
        let speaker = normalize_speaker_alias(speaker.trim());
        let text = text.trim();
        if speaker.is_empty() || text.is_empty() {
            continue;
        }

        if let Some(last) = merged.last_mut() {
            if last.speaker.eq_ignore_ascii_case(&speaker) {
                let needs_period = !last.text.ends_with(['.', '!', '?', ':', ';']);
                if needs_period {
                    last.text.push('.');
                }
                last.text.push(' ');
                last.text.push_str(text);
                continue;
            }
        }

        merged.push(BookLine {
            speaker,
            text: text.to_string(),
        });
    }

    merged
}

fn serialize_book_lines(lines: &[BookLine]) -> String {
    lines.iter()
        .map(|line| format!("{}: {}", line.speaker.trim(), line.text.trim()))
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn normalize_speaker_alias(speaker: &str) -> String {
    match speaker.trim().to_ascii_lowercase().as_str() {
        "shane hollander" => "Shane".to_string(),
        "ilya rozanov" => "Ilya".to_string(),
        "hollander" => "Shane".to_string(),
        "rozanov" => "Ilya".to_string(),
        other if other == "shane" => "Shane".to_string(),
        other if other == "ilya" => "Ilya".to_string(),
        _ => speaker.trim().to_string(),
    }
}

fn normalize_book_output_aliases(input: &str) -> String {
    let mut out = Vec::new();
    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            out.push(String::new());
            continue;
        }
        if let Some((speaker, text)) = trimmed.split_once(':') {
            out.push(format!("{}: {}", normalize_speaker_alias(speaker), text.trim()));
        } else {
            out.push(trimmed.to_string());
        }
    }
    out.join("\n")
}

fn sanitize_formatted_book_lines(input: &str) -> String {
    let mut out: Vec<(String, String)> = Vec::new();
    for raw_line in input.lines() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((speaker, text)) = trimmed.split_once(':') else {
            if let Some((last_speaker, last_text)) = out.last_mut() {
                if !trimmed.is_empty() && !is_quote_only_fragment(trimmed) {
                    if !last_text.is_empty() && !last_text.ends_with(['.', '!', '?', ':', ';', '"']) {
                        last_text.push(' ');
                    }
                    last_text.push_str(trimmed);
                    *last_text = compact_narration_text(last_text);
                } else if *last_speaker == "narrator" {
                    last_text.push('"');
                }
            }
            continue;
        };
        let speaker = speaker.trim();
        let text = text.trim().trim_matches(|ch: char| ch == '\u{feff}');
        if text.is_empty() || is_quote_only_fragment(text) {
            continue;
        }
        if speaker.eq_ignore_ascii_case("narrator") || is_valid_speaker_label(speaker) {
            let speaker = if speaker.eq_ignore_ascii_case("narrator") {
                "narrator".to_string()
            } else {
                speaker.to_string()
            };

            if let Some((last_speaker, last_text)) = out.last_mut() {
                if last_speaker.eq_ignore_ascii_case(&speaker) {
                    if !last_text.ends_with(['.', '!', '?', ':', ';', '"']) {
                        last_text.push(' ');
                    } else {
                        last_text.push(' ');
                    }
                    last_text.push_str(text);
                    if speaker.eq_ignore_ascii_case("narrator") {
                        *last_text = compact_narration_text(last_text);
                    }
                    continue;
                }
            }

            out.push((speaker, text.to_string()));
        }
    }

    out.into_iter()
        .filter_map(|(speaker, text)| {
            let cleaned = if speaker.eq_ignore_ascii_case("narrator") {
                dedupe_repeated_narrator_text(&compact_narration_text(&text))
            } else {
                text.trim().to_string()
            };
            if cleaned.is_empty() || is_quote_only_fragment(&cleaned) {
                None
            } else {
                Some(format!("{}: {}", speaker, cleaned))
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn dedupe_repeated_narrator_text(text: &str) -> String {
    let chunks = split_source_chunks(text);
    if chunks.len() <= 1 {
        return text.trim().to_string();
    }

    let mut seen = Vec::new();
    let mut kept = Vec::new();
    for chunk in chunks {
        let norm = normalize_compare_text(&chunk);
        if norm.len() >= 24 && seen.iter().any(|item: &String| item == &norm) {
            continue;
        }
        seen.push(norm);
        kept.push(chunk);
    }

    if kept.is_empty() {
        text.trim().to_string()
    } else {
        kept.join(" ")
    }
}

fn is_quote_only_fragment(text: &str) -> bool {
    let cleaned = text
        .trim()
        .trim_matches(|ch: char| matches!(ch, '"' | '\u{201C}' | '\u{201D}'))
        .trim();
    cleaned.is_empty()
}

fn is_valid_speaker_label(speaker: &str) -> bool {
    let trimmed = speaker.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.len() == 1 {
        return false;
    }
    if trimmed.contains([',', ';', '!', '?', ':']) {
        return false;
    }

    let words = trimmed.split_whitespace().collect::<Vec<_>>();
    if words.is_empty() || words.len() > 4 {
        return false;
    }

    let invalid_single_word = [
        "a", "an", "and", "as", "at", "because", "before", "but", "he", "her", "his", "if",
        "it", "she", "someone", "something", "that", "the", "their", "then", "they", "this",
        "using", "we", "when", "while", "you",
    ];
    if words.len() == 1 && invalid_single_word.contains(&trimmed.to_ascii_lowercase().as_str()) {
        return false;
    }

    true
}

fn repair_book_dialogue_blocks(input: &str) -> String {
    let mut lines = Vec::new();
    for raw_line in input.lines() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some((speaker, text)) = trimmed.split_once(':') {
            lines.push(format!("{}: {}", speaker.trim(), text.trim()));
        } else {
            lines.push(trimmed.to_string());
        }
    }

    let mut out = Vec::new();
    let mut index = 0usize;
    while index < lines.len() {
        let line = lines[index].clone();
        let Some((speaker, text)) = line.split_once(':') else {
            out.push(line);
            index += 1;
            continue;
        };

        if !speaker.trim().eq_ignore_ascii_case("narrator") {
            out.push(line);
            index += 1;
            continue;
        }

        if let Some((narration, dialogue)) = split_narrator_quote_from_text(text.trim()) {
            let guessed_speaker = guess_named_speaker_from_narration(&narration);

            if let Some(next_line) = lines.get(index + 1) {
                if let Some((next_speaker, next_text)) = next_line.split_once(':') {
                    if normalize_compare_text(next_text.trim()) == normalize_compare_text(&dialogue) {
                        if !narration.trim().is_empty() {
                            out.push(format!("narrator: {}", narration.trim()));
                        }
                        out.push(format!(
                            "{}: {}",
                            guessed_speaker
                                .clone()
                                .unwrap_or_else(|| normalize_speaker_alias(next_speaker.trim())),
                            dialogue.trim()
                        ));
                        index += 2;
                        continue;
                    }
                }
            }

            if let Some(guessed_speaker) = guessed_speaker {
                if !narration.trim().is_empty() {
                    out.push(format!("narrator: {}", narration.trim()));
                }
                out.push(format!("{}: {}", guessed_speaker, dialogue.trim()));
                index += 1;
                continue;
            }
        }

        out.push(line);
        index += 1;
    }

    out.join("\n\n")
}

fn split_narrator_quote_from_text(text: &str) -> Option<(String, String)> {
    let normalized = text.trim();
    let quote_positions = normalized
        .char_indices()
        .filter(|(_, ch)| matches!(*ch, '"' | '\u{201C}' | '\u{201D}'))
        .map(|(idx, ch)| (idx, ch.len_utf8()));

    let positions = quote_positions.collect::<Vec<_>>();
    let (open_idx, open_len) = *positions.first()?;
    let (close_idx, close_len) = *positions.last().unwrap_or(&(open_idx, open_len));
    if close_idx <= open_idx {
        return None;
    }

    let dialogue = normalized[open_idx + open_len..close_idx]
        .trim()
        .trim_matches(|ch: char| matches!(ch, '"' | '\u{201C}' | '\u{201D}'))
        .trim()
        .to_string();
    if dialogue.is_empty() {
        return None;
    }

    let before = normalized[..open_idx].trim();
    let after = normalized[close_idx + close_len..].trim();
    let narration = compact_narration_text(&format!("{before} {after}"));
    Some((narration, dialogue))
}

fn compact_narration_text(input: &str) -> String {
    input
        .replace(" ,", ",")
        .replace(" .", ".")
        .replace(" !", "!")
        .replace(" ?", "?")
        .replace(" ;", ";")
        .replace(" :", ":")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(|ch: char| matches!(ch, '"' | '\u{201C}' | '\u{201D}'))
        .trim_end_matches(',')
        .trim()
        .to_string()
}

fn guess_named_speaker_from_narration(narration: &str) -> Option<String> {
    let trimmed = narration.trim();
    if trimmed.is_empty() {
        return None;
    }

    let first_word = trimmed.split_whitespace().next()?;
    let lower_first = first_word.to_ascii_lowercase();
    if matches!(
        lower_first.as_str(),
        "he" | "she" | "they" | "we" | "i" | "you" | "his" | "her" | "their" | "someone"
    ) {
        return None;
    }

    if !first_word
        .chars()
        .next()
        .map(|ch| ch.is_uppercase())
        .unwrap_or(false)
    {
        return None;
    }

    let mut candidate = Vec::new();
    for token in trimmed.split_whitespace().take(3) {
        let cleaned = token.trim_matches(|ch: char| !ch.is_alphanumeric() && ch != '\'' && ch != '-');
        if cleaned.is_empty() {
            break;
        }
        if !cleaned
            .chars()
            .next()
            .map(|ch| ch.is_uppercase())
            .unwrap_or(false)
        {
            break;
        }
        candidate.push(cleaned);
    }

    if candidate.is_empty() {
        None
    } else {
        let candidate = normalize_speaker_alias(&candidate.join(" "));
        is_valid_speaker_label(&candidate).then_some(candidate)
    }
}

fn recover_missing_source_chunks(source_text: &str, formatted: &str) -> String {
    let source_chunks = split_source_chunks(source_text);
    if source_chunks.is_empty() {
        return formatted.trim().to_string();
    }

    let source_norm = normalize_compare_text(source_text);
    let formatted_norm = normalize_compare_text(formatted);
    if !source_norm.is_empty() {
        let coverage = formatted_norm.len() as f32 / source_norm.len().max(1) as f32;
        if coverage >= 0.85 {
            return formatted.trim().to_string();
        }
    }

    let blocks = formatted
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    if blocks.is_empty() {
        return source_chunks
            .into_iter()
            .map(|chunk| format!("narrator: {}", chunk.trim()))
            .collect::<Vec<_>>()
            .join("\n\n");
    }

    let source_norms = source_chunks
        .iter()
        .map(|chunk| normalize_compare_text(chunk))
        .collect::<Vec<_>>();
    let block_chunk_indices = blocks
        .iter()
        .map(|block| find_best_source_chunk_index(block, &source_norms))
        .collect::<Vec<_>>();

    let mut covered = vec![false; source_chunks.len()];
    for index in block_chunk_indices.iter().flatten() {
        if *index < covered.len() {
            covered[*index] = true;
        }
    }

    let mut next_expected = 0usize;
    let mut out = Vec::new();

    for (block, matched_index) in blocks.iter().zip(block_chunk_indices.iter()) {
        if let Some(matched_index) = matched_index {
            while next_expected < *matched_index {
                if !covered[next_expected] {
                    out.push(format!("narrator: {}", source_chunks[next_expected].trim()));
                }
                next_expected += 1;
            }
            if next_expected <= *matched_index {
                next_expected = matched_index + 1;
            }
        }
        out.push(block.clone());
    }

    while next_expected < source_chunks.len() {
        if !covered[next_expected] {
            out.push(format!("narrator: {}", source_chunks[next_expected].trim()));
        }
        next_expected += 1;
    }

    out.join("\n\n")
}

fn find_best_source_chunk_index(block: &str, source_norms: &[String]) -> Option<usize> {
    let block_text = block
        .split_once(':')
        .map(|(_, text)| text.trim())
        .unwrap_or(block.trim());
    let block_norm = normalize_compare_text(block_text);
    if block_norm.is_empty() {
        return None;
    }

    let mut best: Option<(usize, usize)> = None;
    for (index, source_norm) in source_norms.iter().enumerate() {
        if source_norm.is_empty() {
            continue;
        }

        let matches = source_norm.contains(&block_norm)
            || block_norm.contains(source_norm)
            || shared_prefix_len(&block_norm, source_norm) >= 24;
        if !matches {
            continue;
        }

        let score = block_norm.len().abs_diff(source_norm.len());
        match best {
            Some((_, best_score)) if score >= best_score => {}
            _ => best = Some((index, score)),
        }
    }

    best.map(|(index, _)| index)
}

fn shared_prefix_len(left: &str, right: &str) -> usize {
    left.chars()
        .zip(right.chars())
        .take_while(|(a, b)| a == b)
        .count()
}

fn split_source_chunks(input: &str) -> Vec<String> {
    let normalized = input.replace("\r\n", "\n");
    let mut chunks = Vec::new();
    let mut current = String::new();

    for ch in normalized.chars() {
        current.push(ch);
        if matches!(ch, '.' | '!' | '?') {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                chunks.push(trimmed.to_string());
            }
            current.clear();
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        chunks.push(trimmed.to_string());
    }

    chunks
}

fn normalize_compare_text(input: &str) -> String {
    input
        .replace(['\u{201C}', '\u{201D}'], "\"")
        .replace(['\u{2018}', '\u{2019}'], "'")
        .replace('\u{2014}', "-")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn split_tts_segments(input: &str) -> Vec<String> {
    let normalized = normalize_gemini_tts_text(input)
        .replace("\r\n", "\n")
        .replace('\n', " ");
    let compact = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        return Vec::new();
    }

    let soft_limit = 220usize;
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut last_space_idx: Option<usize> = None;

    for ch in compact.chars() {
        current.push(ch);
        if ch.is_whitespace() {
            last_space_idx = Some(current.len());
        }

        let is_sentence_break = matches!(ch, '.' | '!' | '?' | ';' | ':');
        if is_sentence_break && current.trim().chars().count() >= 24 {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                segments.push(trimmed.to_string());
            }
            current.clear();
            last_space_idx = None;
            continue;
        }

        if current.chars().count() >= soft_limit {
            if let Some(split_at) = last_space_idx {
                let remainder = current.split_off(split_at);
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    segments.push(trimmed.to_string());
                }
                current = remainder.trim_start().to_string();
                last_space_idx = None;
            } else {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    segments.push(trimmed.to_string());
                }
                current.clear();
                last_space_idx = None;
            }
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        segments.push(trimmed.to_string());
    }
    segments
}

fn append_samples_with_pause(
    output: &mut Vec<i16>,
    input: &[i16],
    add_pause: bool,
    pause_ms: u32,
) {
    if add_pause && !output.is_empty() {
        let silence = (SAMPLE_RATE as u64 * pause_ms as u64 / 1000) as usize;
        output.extend(std::iter::repeat(0i16).take(silence));
    }
    output.extend_from_slice(input);
}

fn current_timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn export_dir_for_session() -> PathBuf {
    PathBuf::from(EXPORT_DIR)
}

fn next_project_root_dir() -> Result<PathBuf> {
    let root = export_dir_for_session();
    fs::create_dir_all(&root)
        .with_context(|| format!("Kh?ng tao duoc {}", root.display()))?;
    let mut max_index = 0u32;
    for entry in fs::read_dir(&root)
        .with_context(|| format!("Kh?ng doc duoc {}", root.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(index) = name.parse::<u32>() {
                max_index = max_index.max(index);
            }
        }
    }
    Ok(root.join((max_index + 1).to_string()))
}

fn project_text_dir(root: &Path) -> PathBuf {
    root.join("text")
}

fn project_audio_dir(root: &Path) -> PathBuf {
    root.join("audio")
}

fn clear_directory_files(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)
        .with_context(|| format!("Could not read {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_file() {
            fs::remove_file(&path)
                .with_context(|| format!("Could not remove {}", path.display()))?;
        }
    }
    Ok(())
}

fn project_audio_lines_dir(root: &Path) -> PathBuf {
    root.join("audio").join("lines")
}

fn project_subtitle_dir(root: &Path) -> PathBuf {
    root.join("subtitles")
}

fn project_video_dir(root: &Path) -> PathBuf {
    root.join("video")
}

fn project_video_preview_dir(root: &Path) -> PathBuf {
    root.join("preview")
}

fn project_video_final_dir(root: &Path) -> PathBuf {
    root.join("video")
}

fn project_image_dir(root: &Path) -> PathBuf {
    root.join("images")
}

fn ensure_project_structure(root: &Path) -> Result<()> {
    for dir in [
        root.to_path_buf(),
        project_text_dir(root),
        project_audio_dir(root),
        project_audio_lines_dir(root),
        project_subtitle_dir(root),
        project_video_dir(root),
        project_video_preview_dir(root),
        project_video_final_dir(root),
        project_image_dir(root),
    ] {
        fs::create_dir_all(&dir).with_context(|| format!("Kh?ng tao duoc {}", dir.display()))?;
    }
    Ok(())
}

fn normalize_project_root(path: &Path) -> PathBuf {
    let mut current = if path.is_file() {
        path.parent().map(Path::to_path_buf).unwrap_or_else(|| path.to_path_buf())
    } else {
        path.to_path_buf()
    };
    loop {
        let Some(name) = current.file_name().and_then(|name| name.to_str()) else {
            break;
        };
        let lower = name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "video" | "preview" | "audio" | "lines" | "subtitles" | "images" | "text"
        ) {
            if let Some(parent) = current.parent() {
                current = parent.to_path_buf();
                continue;
            }
        }
        break;
    }
    current
}

fn write_project_text_files(root: &Path, source_text: &str, formatted_text: &str) -> Result<()> {
    ensure_project_structure(root)?;
    fs::write(project_text_dir(root).join("text_goc.txt"), source_text)
        .with_context(|| "Kh?ng ghi duoc text goc".to_string())?;
    fs::write(project_text_dir(root).join("text_tach_narrator_thoai.txt"), formatted_text)
        .with_context(|| "Kh?ng ghi duoc text ?? tach".to_string())?;
    Ok(())
}

fn copy_background_to_project(root: &Path, background_path: Option<&Path>) -> Result<Option<PathBuf>> {
    let Some(path) = background_path else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }
    ensure_project_structure(root)?;
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("png");
    let destination = project_image_dir(root).join(format!("background.{}", extension));
    if normalize_separators(path) == normalize_separators(&destination) {
        return Ok(Some(destination));
    }
    fs::copy(path, &destination).with_context(|| {
        format!(
            "Kh?ng copy duoc anh nen {} -> {}",
            path.display(),
            destination.display()
        )
    })?;
    Ok(Some(destination))
}

fn normalize_separators(path: &Path) -> String {
    path.to_string_lossy().replace('/', "\\").to_ascii_lowercase()
}

fn next_numbered_file_path(dir: &Path, prefix: &str, extension: &str) -> Result<PathBuf> {
    fs::create_dir_all(dir).with_context(|| format!("Kh?ng tao duoc {}", dir.display()))?;
    let mut max_index = 0u32;
    for entry in fs::read_dir(dir).with_context(|| format!("Kh?ng doc duoc {}", dir.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let entry_path = entry.path();
        let Some(stem) = entry_path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let expected_prefix = format!("{}_", prefix);
        if let Some(rest) = stem.strip_prefix(&expected_prefix) {
            if let Some(first) = rest.split('_').next() {
                if let Ok(index) = first.parse::<u32>() {
                    max_index = max_index.max(index);
                }
            }
        }
    }
    Ok(dir.join(format!("{}_{:04}.{}", prefix, max_index + 1, extension)))
}

fn latest_srt_in_project(root: &Path) -> Option<PathBuf> {
    let dir = project_subtitle_dir(root);
    let mut candidates = fs::read_dir(&dir)
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let is_srt = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("srt"));
            if !is_srt {
                return None;
            }
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some((modified, path))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| a.0.cmp(&b.0));
    candidates.pop().map(|(_, path)| path)
}

fn list_available_project_dirs() -> Result<Vec<PathBuf>> {
    let root = export_dir_for_session();
    fs::create_dir_all(&root)
        .with_context(|| format!("Kh?ng tao duoc {}", root.display()))?;
    let mut projects = fs::read_dir(&root)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            if !entry.file_type().ok()?.is_dir() {
                return None;
            }
            let name = path.file_name().and_then(|v| v.to_str()).unwrap_or_default();
            let looks_like_project = name.parse::<u32>().is_ok()
                || project_text_dir(&path).exists()
                || project_audio_dir(&path).exists()
                || project_subtitle_dir(&path).exists();
            looks_like_project.then_some(path)
        })
        .collect::<Vec<_>>();
    projects.sort_by(|a, b| {
        let a_name = a.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        let b_name = b.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        match (a_name.parse::<u32>(), b_name.parse::<u32>()) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            _ => a_name.cmp(b_name),
        }
    });
    Ok(projects)
}

fn latest_file_in_dir_matching<F>(dir: &Path, mut predicate: F) -> Option<PathBuf>
where
    F: FnMut(&Path) -> bool,
{
    let mut candidates = fs::read_dir(dir)
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            if !predicate(&path) {
                return None;
            }
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some((modified, path))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| a.0.cmp(&b.0));
    candidates.pop().map(|(_, path)| path)
}

fn find_latest_audiobook_in_project(root: &Path) -> Option<PathBuf> {
    latest_file_in_dir_matching(&project_audio_dir(root), |path| {
        path.extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("wav"))
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("audio_"))
    })
}

fn latest_background_in_project(root: &Path) -> Option<PathBuf> {
    latest_file_in_dir_matching(&project_image_dir(root), |path| {
        path.extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| {
                matches!(
                    ext.to_ascii_lowercase().as_str(),
                    "png" | "jpg" | "jpeg" | "webp" | "bmp"
                )
            })
    })
}

fn latest_video_in_project(root: &Path) -> Option<PathBuf> {
    latest_file_in_dir_matching(&project_video_final_dir(root), |path| {
        path.extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("mp4"))
    })
}

fn rebuild_project_speeches(root: &Path, book_lines: &[BookLine]) -> Vec<ExportedSpeech> {
    let lines_dir = project_audio_lines_dir(root);
    book_lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            let path = lines_dir.join(format!("{:04}.wav", index + 1));
            path.exists().then(|| ExportedSpeech {
                index,
                speaker: line.speaker.clone(),
                text: line.text.clone(),
                audio_path: path.to_string_lossy().to_string(),
                applied_gain_percent: 100,
            })
        })
        .collect()
}

fn write_samples_to_wav(path: &Path, samples: &[i16]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Kh?ng tao duoc thu muc {}", parent.display()))?;
    }

    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: SAMPLE_RATE,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };

    let mut writer = hound::WavWriter::create(path, spec)
        .with_context(|| format!("Kh?ng tao duoc file {}", path.display()))?;
    for sample in samples {
        writer
            .write_sample(*sample)
            .with_context(|| format!("Loi ghi WAV {}", path.display()))?;
    }
    writer
        .finalize()
        .with_context(|| format!("Loi finalize WAV {}", path.display()))?;
    Ok(())
}

fn merge_wav_files(inputs: &[PathBuf], output: &Path, pause_ms: u32) -> Result<()> {
    if inputs.is_empty() {
        return Err(anyhow!("Kh?ng co file WAV de ghep."));
    }

    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Kh?ng tao duoc thu muc {}", parent.display()))?;
    }

    let mut first_reader = hound::WavReader::open(&inputs[0])
        .with_context(|| format!("Kh?ng mo duoc WAV {}", inputs[0].display()))?;
    let spec = first_reader.spec();
    let mut writer = hound::WavWriter::create(output, spec)
        .with_context(|| format!("Kh?ng tao duoc file {}", output.display()))?;

    for sample in first_reader.samples::<i16>() {
        writer
            .write_sample(sample.with_context(|| {
                format!("Loi doc sample {}", inputs[0].display())
            })?)
            .with_context(|| format!("Loi ghi audiobook {}", output.display()))?;
    }

    let silence = (SAMPLE_RATE as u64 * pause_ms as u64 / 1000) as usize;
    for path in &inputs[1..] {
        for _ in 0..silence {
            writer
                .write_sample(0i16)
                .with_context(|| format!("Loi ghi audiobook {}", output.display()))?;
        }
        let mut reader = hound::WavReader::open(path)
            .with_context(|| format!("Kh?ng mo duoc WAV {}", path.display()))?;
        if reader.spec() != spec {
            return Err(anyhow!(
                "Kh?ng the ghep WAV khac sample rate/channels: {}",
                path.display()
            ));
        }
        for sample in reader.samples::<i16>() {
            writer
                .write_sample(
                    sample.with_context(|| format!("Loi doc sample {}", path.display()))?,
                )
                .with_context(|| format!("Loi ghi audiobook {}", output.display()))?;
        }
    }

    writer
        .finalize()
        .with_context(|| format!("Loi finalize audiobook {}", output.display()))?;
    Ok(())
}

fn build_timed_book_lines(
    lines: &[BookLine],
    speeches: &[ExportedSpeech],
    pause_ms: u32,
) -> Result<Vec<TimedBookLine>> {
    let mut timed = Vec::with_capacity(lines.len());
    let mut current_start = 0.0f64;
    for (index, line) in lines.iter().enumerate() {
        let speech = speeches
            .iter()
            .find(|item| item.index == index)
            .ok_or_else(|| anyhow!("Thieu cache WAV o dong {}", index + 1))?;
        let samples = read_wav_samples(Path::new(&speech.audio_path))?;
        let duration_seconds = samples.len() as f64 / SAMPLE_RATE as f64;
        let end_seconds = current_start + duration_seconds;
        timed.push(TimedBookLine {
            speaker: line.speaker.clone(),
            text: line.text.clone(),
            start_seconds: current_start,
            end_seconds: end_seconds + (pause_ms as f64 / 1000.0),
        });
        current_start = end_seconds + (pause_ms as f64 / 1000.0);
    }
    Ok(timed)
}

#[derive(Clone)]
struct SentenceItem {
    speaker: String,
    text: String,
    fallback_start_seconds: f64,
    fallback_end_seconds: f64,
}

#[derive(Deserialize)]
struct GeminiSubtitlePayload {
    segments: Vec<GeminiSubtitleSegment>,
}

#[derive(Deserialize)]
struct GeminiSubtitleSegment {
    sentence_index: usize,
    start_ms: u64,
    end_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct GeminiWordTimingPayload {
    words: Vec<GeminiWordTimingSegment>,
}

#[derive(Serialize, Deserialize)]
struct GeminiWordTimingSegment {
    line_index: usize,
    word_index: usize,
    start_ms: u64,
    end_ms: u64,
}

#[derive(Clone)]
struct WordItem {
    line_index: usize,
    word_index: usize,
    word: String,
    fallback_start_seconds: f64,
    fallback_end_seconds: f64,
}

fn collect_sentence_items(
    lines: &[BookLine],
    speeches: &[ExportedSpeech],
    pause_ms: u32,
) -> Result<Vec<SentenceItem>> {
    let timed_lines = build_timed_book_lines(lines, speeches, pause_ms)?;
    let mut items = Vec::new();
    for line in timed_lines {
        let sentences = split_text_into_sentences(&line.text);
        if sentences.is_empty() {
            continue;
        }
        let total_chars = sentences.iter().map(|item| item.chars().count()).sum::<usize>().max(1);
        let total_duration = (line.end_seconds - line.start_seconds).max(0.2);
        let mut current_start = line.start_seconds;
        for (idx, sentence) in sentences.iter().enumerate() {
            let ratio = sentence.chars().count() as f64 / total_chars as f64;
            let is_last = idx + 1 == sentences.len();
            let next_end = if is_last {
                line.end_seconds
            } else {
                (current_start + total_duration * ratio).min(line.end_seconds)
            };
            items.push(SentenceItem {
                speaker: line.speaker.clone(),
                text: sentence.clone(),
                fallback_start_seconds: current_start,
                fallback_end_seconds: next_end.max(current_start + 0.12),
            });
            current_start = next_end;
        }
    }
    Ok(items)
}

fn collect_word_items(timed_lines: &[TimedBookLine]) -> Vec<WordItem> {
    let mut items = Vec::new();
    for (line_index, line) in timed_lines.iter().enumerate() {
        let words = split_text_into_words(&line.text);
        if words.is_empty() {
            continue;
        }
        let total_chars = words.iter().map(|word| word.chars().count()).sum::<usize>().max(1);
        let total_duration = (line.end_seconds - line.start_seconds).max(0.12);
        let mut current_start = line.start_seconds;
        for (word_index, word) in words.iter().enumerate() {
            let ratio = word.chars().count() as f64 / total_chars as f64;
            let is_last = word_index + 1 == words.len();
            let next_end = if is_last {
                line.end_seconds
            } else {
                (current_start + total_duration * ratio).min(line.end_seconds)
            };
            items.push(WordItem {
                line_index,
                word_index,
                word: word.clone(),
                fallback_start_seconds: current_start,
                fallback_end_seconds: next_end.max(current_start + 0.06),
            });
            current_start = next_end;
        }
    }
    items
}

fn split_text_into_sentences(text: &str) -> Vec<String> {
    let normalized = text.replace('\n', " ");
    let chars: Vec<char> = normalized.chars().collect();
    let mut sentences = Vec::new();
    let mut current = String::new();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        current.push(ch);
        let mut boundary = matches!(ch, '.' | '!' | '?');
        if ch == '\u{2026}' {
            boundary = true;
        }
        if ch == '.' && i + 2 < chars.len() && chars[i + 1] == '.' && chars[i + 2] == '.' {
            current.push('.');
            current.push('.');
            i += 2;
            boundary = true;
        }

        if boundary {
            while i + 1 < chars.len() && matches!(chars[i + 1], '"' | '\'' | ')' | '\u{201D}') {
                i += 1;
                current.push(chars[i]);
            }
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                sentences.push(trimmed.to_string());
            }
            current.clear();
        }
        i += 1;
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        sentences.push(trimmed.to_string());
    }
    sentences
}

fn split_text_into_words(text: &str) -> Vec<String> {
    text.split_whitespace()
        .map(str::trim)
        .filter(|word| !word.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn subtitle_prefix(speaker: &str) -> String {
    let trimmed = speaker.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("narrator") {
        String::new()
    } else {
        format!("{}: ", trimmed)
    }
}

fn display_subtitle_text(speaker: &str, text: &str) -> String {
    format!("{}{}", subtitle_prefix(speaker), text.trim())
}

fn split_prefixed_subtitle_text(text: &str) -> (String, String) {
    let trimmed = text.trim();
    if let Some((left, right)) = trimmed.split_once(':') {
        let speaker = left.trim();
        let body = right.trim();
        if !speaker.is_empty()
            && !body.is_empty()
            && speaker.len() <= 40
            && speaker
                .chars()
                .all(|ch| ch.is_alphanumeric() || ch.is_whitespace() || ch == '\'' || ch == '-')
        {
            return (speaker.to_string(), body.to_string());
        }
    }
    (String::new(), trimmed.to_string())
}

fn word_timing_path_for_srt(srt_path: &Path) -> PathBuf {
    let parent = srt_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = srt_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("subtitles");
    parent.join(format!("{}.words.json", stem))
}

fn load_word_timing_file(path: &Path) -> Result<Vec<TimedWordSegment>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Kh?ng doc duoc word timing {}", path.display()))?;
    serde_json::from_str(&content)
        .with_context(|| format!("Word timing JSON khong hop le: {}", path.display()))
}

fn compress_audio_for_gemini(input: &Path, output: &Path) -> Result<()> {
    let ffmpeg = find_ffmpeg()?;
    let mut command = Command::new(ffmpeg);
    command
        .arg("-y")
        .arg("-i")
        .arg(input)
        .args(["-vn", "-ac", "1", "-ar", "16000", "-b:a", "48k"])
        .arg(output);
    let output_result = command.output().context("Kh?ng goi duoc ffmpeg de nen audio cho Gemini")?;
    if !output_result.status.success() {
        return Err(anyhow!(
            "Nen audio cho Gemini that bai: {}",
            String::from_utf8_lossy(&output_result.stderr)
        ));
    }
    Ok(())
}

fn analyze_sentence_timing_with_gemini(
    api_key: &str,
    audio_path: &Path,
    sentence_items: &[SentenceItem],
) -> Result<Vec<TimedBookLine>> {
    let mime_type = infer_audio_mime_type(audio_path)
        .ok_or_else(|| anyhow!("Dinh ?ang audio nen khong duoc Gemini ho tro."))?;
    let bytes = fs::read(audio_path)
        .with_context(|| format!("Kh?ng doc duoc audio {}", audio_path.display()))?;
    if bytes.len() > MAX_INLINE_AUDIO_BYTES {
        return Err(anyhow!(
            "Audio cho Gemini van qua lon sau khi nen: {} MB",
            bytes.len() / (1024 * 1024)
        ));
    }

    let sentence_list = sentence_items
        .iter()
        .enumerate()
        .map(|(idx, item)| format!("{}. {}", idx + 1, item.text))
        .collect::<Vec<_>>()
        .join("\n");

    let payload = serde_json::json!({
        "contents": [{
            "parts": [
                {
                    "text": format!(
                        "You are aligning subtitle timing for an audiobook. The audio matches the provided sentence list exactly and in the same order. Return valid JSON only with this shape: {{\"segments\":[{{\"sentence_index\":1,\"start_ms\":0,\"end_ms\":1200}}]}}. Rules: use every sentence exactly once; preserve order; do not rewrite text; start_ms/end_ms are integers in milliseconds; end_ms must be greater than start_ms; segments must be monotonic and must cover the whole spoken audio. Sentence list:\n{}",
                        sentence_list
                    )
                },
                {
                    "inline_data": {
                        "mime_type": mime_type,
                        "data": general_purpose::STANDARD.encode(bytes),
                    }
                }
            ]
        }],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json"
        }
    });

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        ANALYSIS_MODEL, api_key
    );

    let response = ureq::post(&url)
        .content_type("application/json")
        .send(&payload.to_string())
        .map_err(|err| anyhow!("Kh?ng goi duoc Gemini de tao SRT: {}", err))?;
    let body = response
        .into_body()
        .read_to_string()
        .map_err(|err| anyhow!("Kh?ng doc duoc phan hoi Gemini SRT: {}", err))?;
    let json: Value =
        serde_json::from_str(&body).map_err(|err| anyhow!("JSON Gemini SRT khong hop le: {}", err))?;
    let raw_text = extract_candidate_text(&json)
        .ok_or_else(|| anyhow!("Gemini khong tra ve SRT timing hop le."))?;
    let parsed: GeminiSubtitlePayload = serde_json::from_str(&raw_text)
        .map_err(|err| anyhow!("JSON segment timing khong hop le: {}", err))?;

    if parsed.segments.len() != sentence_items.len() {
        return Err(anyhow!(
            "Sentence timing mismatch: Gemini returned {} segments for {} sentences. One sentence was likely merged, skipped, or split differently.",
            parsed.segments.len(),
            sentence_items.len()
        ));
    }

    let mut out = Vec::with_capacity(sentence_items.len());
    for (idx, item) in sentence_items.iter().enumerate() {
        let segment = &parsed.segments[idx];
        if segment.sentence_index != idx + 1 {
            return Err(anyhow!(
                "Gemini tra sai thu tu subtitle o cau {}.",
                idx + 1
            ));
        }
        let start_seconds = segment.start_ms as f64 / 1000.0;
        let mut end_seconds = segment.end_ms as f64 / 1000.0;
        if end_seconds <= start_seconds {
            end_seconds = item.fallback_end_seconds.max(start_seconds + 0.12);
        }
        out.push(TimedBookLine {
            speaker: item.speaker.clone(),
            text: item.text.clone(),
            start_seconds,
            end_seconds,
        });
    }

    sanitize_timed_lines(&mut out);
    Ok(out)
}

fn sanitize_timed_lines(lines: &mut [TimedBookLine]) {
    if lines.is_empty() {
        return;
    }

    for index in 0..lines.len() {
        let start = lines[index].start_seconds.max(0.0);
        let mut end = lines[index].end_seconds.max(start + 0.12);
        let word_count = lines[index]
            .text
            .split_whitespace()
            .filter(|token| !token.trim().is_empty())
            .count()
            .max(1) as f64;
        let hard_max_duration = (word_count * 0.70 + 2.0).clamp(2.5, 10.0);
        let next_start = lines
            .get(index + 1)
            .map(|line| line.start_seconds)
            .unwrap_or(f64::INFINITY);
        let max_by_neighbor = if next_start.is_finite() {
            (next_start - 0.05).max(start + 0.20)
        } else {
            start + hard_max_duration
        };
        let max_reasonable_end = (start + hard_max_duration).min(max_by_neighbor);
        if end - start > hard_max_duration {
            end = max_reasonable_end.max(start + 0.20);
        }
        if next_start.is_finite() && end >= next_start {
            end = (next_start - 0.05).max(start + 0.20);
        }
        lines[index].start_seconds = start;
        lines[index].end_seconds = end.max(start + 0.12);
    }
}

fn apply_subtitle_lead_to_lines(lines: &mut [TimedBookLine], lead_seconds: f64) {
    if lines.is_empty() || lead_seconds <= 0.0 {
        return;
    }

    for line in lines.iter_mut() {
        let shifted_start = (line.start_seconds - lead_seconds).max(0.0);
        let shifted_end = (line.end_seconds - lead_seconds).max(shifted_start + 0.12);
        line.start_seconds = shifted_start;
        line.end_seconds = shifted_end.max(shifted_start + 0.12);
    }
    sanitize_timed_lines(lines);
}

fn apply_subtitle_lead_to_word_segments(segments: &mut [TimedWordSegment], lead_seconds: f64) {
    if segments.is_empty() || lead_seconds <= 0.0 {
        return;
    }

    for segment in segments.iter_mut() {
        let shifted_start = (segment.start_seconds - lead_seconds).max(0.0);
        let shifted_end = (segment.end_seconds - lead_seconds).max(shifted_start + 0.04);
        segment.start_seconds = shifted_start;
        segment.end_seconds = shifted_end.max(shifted_start + 0.04);
    }
}

fn ensure_not_cancelled(cancel_flag: &AtomicBool, message: &str) -> Result<()> {
    if cancel_flag.load(Ordering::SeqCst) {
        return Err(anyhow!(message.to_string()));
    }
    Ok(())
}

fn build_video_srt_with_gemini(
    api_key: &str,
    lines: &[BookLine],
    speeches: &[ExportedSpeech],
    audiobook_path: &Path,
    output_dir: &Path,
    pause_ms: u32,
    enable_word_highlight: bool,
    subtitle_lead_seconds: f64,
    cancel_flag: &AtomicBool,
    progress_tx: Sender<VideoEvent>,
) -> Result<(PathBuf, Vec<TimedBookLine>, Vec<TimedWordSegment>)> {
    ensure_project_structure(output_dir)?;
    ensure_not_cancelled(cancel_flag, "Video export cancelled.")?;
    let _ = progress_tx.send(VideoEvent::Progress {
        fraction: 0.05,
        label: "?ang chu?n b? audio cho Gemini...".to_string(),
    });
    let compressed_audio_path = project_subtitle_dir(output_dir).join("audiobook_for_gemini.mp3");
    compress_audio_for_gemini(audiobook_path, &compressed_audio_path)?;
    ensure_not_cancelled(cancel_flag, "SRT generation cancelled.")?;
    let sentence_items = collect_sentence_items(lines, speeches, pause_ms)?;
    if sentence_items.is_empty() {
        return Err(anyhow!("Kh?ng tao duoc ??nh sach cau cho video."));
    }
    let _ = progress_tx.send(VideoEvent::Progress {
        fraction: 0.16,
        label: "?ang d?ng Gemini t?o timeline SRT...".to_string(),
    });
    let mut timed_lines = analyze_sentence_timing_with_gemini(
        api_key,
        &compressed_audio_path,
        &sentence_items,
    )?;
    ensure_not_cancelled(cancel_flag, "Video export cancelled.")?;
    apply_subtitle_lead_to_lines(&mut timed_lines, subtitle_lead_seconds);
    let mut word_segments = if enable_word_highlight {
        let _ = progress_tx.send(VideoEvent::Progress {
            fraction: 0.23,
            label: "?ang d?ng Gemini t?o word timing...".to_string(),
        });
        let word_items = collect_word_items(&timed_lines);
        let segments =
            analyze_word_timing_with_gemini(api_key, &compressed_audio_path, &word_items)?;
        ensure_not_cancelled(cancel_flag, "Video export cancelled.")?;
        segments
    } else {
        let _ = progress_tx.send(VideoEvent::Progress {
            fraction: 0.23,
            label: "B? qua word timing, ch? d?ng timing theo c?u.".to_string(),
        });
        Vec::new()
    };
    apply_subtitle_lead_to_word_segments(&mut word_segments, subtitle_lead_seconds);
    let srt_path = next_numbered_file_path(&project_subtitle_dir(output_dir), "subtitles", "srt")?;
    fs::write(&srt_path, build_srt_content(&timed_lines))
        .with_context(|| format!("Kh?ng ghi duoc {}", srt_path.display()))?;
    let word_path = word_timing_path_for_srt(&srt_path);
    if enable_word_highlight {
        fs::write(
            &word_path,
            serde_json::to_string_pretty(&word_segments)
                .context("Kh?ng serialise duoc word timing JSON")?,
        )
        .with_context(|| format!("Kh?ng ghi duoc {}", word_path.display()))?;
    } else if word_path.exists() {
        let _ = fs::remove_file(&word_path);
    }
    let _ = progress_tx.send(VideoEvent::Progress {
        fraction: 0.28,
        label: format!("?? t?o SRT: {}", srt_path.display()),
    });
    Ok((srt_path, timed_lines, word_segments))
}

fn build_video_srt_with_gemini_background(
    job_id: u64,
    api_key: &str,
    lines: &[BookLine],
    speeches: &[ExportedSpeech],
    audiobook_path: &Path,
    output_dir: &Path,
    pause_ms: u32,
    enable_word_highlight: bool,
    subtitle_lead_seconds: f64,
    cancel_flag: &AtomicBool,
    progress_tx: Sender<SrtJobEvent>,
) -> Result<(PathBuf, Vec<TimedBookLine>, Vec<TimedWordSegment>)> {
    ensure_project_structure(output_dir)?;
    ensure_not_cancelled(cancel_flag, "SRT generation cancelled.")?;
    let _ = progress_tx.send(SrtJobEvent::Progress {
        job_id,
        fraction: 0.05,
        label: "Preparing audio for Gemini...".to_string(),
    });
    let compressed_audio_path = project_subtitle_dir(output_dir).join("audiobook_for_gemini.mp3");
    compress_audio_for_gemini(audiobook_path, &compressed_audio_path)?;
    ensure_not_cancelled(cancel_flag, "Video export cancelled.")?;
    let sentence_items = collect_sentence_items(lines, speeches, pause_ms)?;
    if sentence_items.is_empty() {
        return Err(anyhow!("Could not prepare sentence list for SRT."));
    }
    let _ = progress_tx.send(SrtJobEvent::Progress {
        job_id,
        fraction: 0.16,
        label: "Creating sentence timing with Gemini...".to_string(),
    });
    let mut timed_lines =
        analyze_sentence_timing_with_gemini(api_key, &compressed_audio_path, &sentence_items)?;
    ensure_not_cancelled(cancel_flag, "SRT generation cancelled.")?;
    apply_subtitle_lead_to_lines(&mut timed_lines, subtitle_lead_seconds);
    let mut word_segments = if enable_word_highlight {
        let _ = progress_tx.send(SrtJobEvent::Progress {
            job_id,
            fraction: 0.23,
            label: "Creating word timing with Gemini...".to_string(),
        });
        let word_items = collect_word_items(&timed_lines);
        let segments =
            analyze_word_timing_with_gemini(api_key, &compressed_audio_path, &word_items)?;
        ensure_not_cancelled(cancel_flag, "SRT generation cancelled.")?;
        segments
    } else {
        let _ = progress_tx.send(SrtJobEvent::Progress {
            job_id,
            fraction: 0.23,
            label: "Skipping word timing. Sentence timing only.".to_string(),
        });
        Vec::new()
    };
    apply_subtitle_lead_to_word_segments(&mut word_segments, subtitle_lead_seconds);
    let srt_path = next_numbered_file_path(&project_subtitle_dir(output_dir), "subtitles", "srt")?;
    fs::write(&srt_path, build_srt_content(&timed_lines))
        .with_context(|| format!("Could not write {}", srt_path.display()))?;
    let word_path = word_timing_path_for_srt(&srt_path);
    if enable_word_highlight {
        fs::write(
            &word_path,
            serde_json::to_string_pretty(&word_segments)
                .context("Could not serialize word timing JSON")?,
        )
        .with_context(|| format!("Could not write {}", word_path.display()))?;
    } else if word_path.exists() {
        let _ = fs::remove_file(&word_path);
    }
    let _ = progress_tx.send(SrtJobEvent::Progress {
        job_id,
        fraction: 1.0,
        label: format!("SRT ready: {}", srt_path.display()),
    });
    Ok((srt_path, timed_lines, word_segments))
}

fn build_srt_content(lines: &[TimedBookLine]) -> String {
    let mut out = String::new();
    for (idx, line) in lines.iter().enumerate() {
        out.push_str(&(idx + 1).to_string());
        out.push('\n');
        out.push_str(&format!(
            "{} --> {}\n",
            format_srt_time(line.start_seconds),
            format_srt_time(line.end_seconds)
        ));
        out.push_str(&display_subtitle_text(&line.speaker, &line.text));
        out.push_str("\n\n");
    }
    out
}

fn format_srt_time(seconds: f64) -> String {
    let total_ms = (seconds.max(0.0) * 1000.0).round() as u64;
    let hours = total_ms / 3_600_000;
    let minutes = (total_ms % 3_600_000) / 60_000;
    let secs = (total_ms % 60_000) / 1000;
    let millis = total_ms % 1000;
    format!("{:02}:{:02}:{:02},{:03}", hours, minutes, secs, millis)
}

fn parse_srt_file(path: &Path) -> Result<Vec<TimedBookLine>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Kh?ng doc duoc SRT {}", path.display()))?;
    let normalized = content.replace("\r\n", "\n");
    let mut out = Vec::new();

    for block in normalized.split("\n\n") {
        let lines = block
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>();
        if lines.len() < 2 {
            continue;
        }

        let timing_line_index = if lines[0].contains("-->") { 0 } else { 1 };
        if timing_line_index >= lines.len() {
            continue;
        }
        let timing_line = lines[timing_line_index];
        let Some((start, end)) = timing_line.split_once("-->") else {
            continue;
        };
        let text_lines = lines
            .iter()
            .skip(timing_line_index + 1)
            .copied()
            .collect::<Vec<_>>();
        if text_lines.is_empty() {
            continue;
        }
        let start_seconds = parse_srt_timestamp(start.trim())
            .with_context(|| format!("Timestamp SRT khong hop le: {}", start.trim()))?;
        let end_seconds = parse_srt_timestamp(end.trim())
            .with_context(|| format!("Timestamp SRT khong hop le: {}", end.trim()))?;
        if end_seconds <= start_seconds {
            continue;
        }

        let merged_text = text_lines.join(" ");
        let (speaker, text) = split_prefixed_subtitle_text(&merged_text);
        out.push(TimedBookLine {
            speaker,
            text,
            start_seconds,
            end_seconds,
        });
    }

    Ok(out)
}

fn analyze_word_timing_with_gemini(
    api_key: &str,
    audio_path: &Path,
    word_items: &[WordItem],
) -> Result<Vec<TimedWordSegment>> {
    let mime_type = infer_audio_mime_type(audio_path)
        .ok_or_else(|| anyhow!("Dinh ?ang audio nen khong duoc Gemini ho tro."))?;
    let bytes = fs::read(audio_path)
        .with_context(|| format!("Kh?ng doc duoc audio {}", audio_path.display()))?;
    if bytes.len() > MAX_INLINE_AUDIO_BYTES {
        return Err(anyhow!(
            "Audio cho Gemini van qua lon sau khi nen: {} MB",
            bytes.len() / (1024 * 1024)
        ));
    }

    let word_list = word_items
        .iter()
        .map(|item| format!(
            "line {} word {} = {}",
            item.line_index + 1,
            item.word_index + 1,
            item.word
        ))
        .collect::<Vec<_>>()
        .join("\n");

    let payload = serde_json::json!({
        "contents": [{
            "parts": [
                {
                    "text": format!(
                        "You are aligning word timing for audiobook subtitles. The audio matches the provided word list exactly and in the same order. Return valid JSON only with this shape: {{\"words\":[{{\"line_index\":1,\"word_index\":1,\"start_ms\":0,\"end_ms\":180}}]}}. Rules: preserve order exactly; use every word exactly once; line_index and word_index are 1-based and must match the provided list; start_ms/end_ms are integers in milliseconds; end_ms must be greater than start_ms; words must monotonically increase and cover the spoken audio. Word list:\n{}",
                        word_list
                    )
                },
                {
                    "inline_data": {
                        "mime_type": mime_type,
                        "data": general_purpose::STANDARD.encode(bytes),
                    }
                }
            ]
        }],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json"
        }
    });

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        ANALYSIS_MODEL, api_key
    );

    let response = ureq::post(&url)
        .content_type("application/json")
        .send(&payload.to_string())
        .map_err(|err| anyhow!("Kh?ng goi duoc Gemini de tao word timing: {}", err))?;
    let body = response
        .into_body()
        .read_to_string()
        .map_err(|err| anyhow!("Kh?ng doc duoc phan hoi Gemini word timing: {}", err))?;
    let json: Value = serde_json::from_str(&body)
        .map_err(|err| anyhow!("JSON Gemini word timing khong hop le: {}", err))?;
    let raw_text = extract_candidate_text(&json)
        .ok_or_else(|| anyhow!("Gemini khong tra ve word timing hop le."))?;
    let parsed: GeminiWordTimingPayload = serde_json::from_str(&raw_text)
        .map_err(|err| anyhow!("JSON word timing khong hop le: {}", err))?;

    if parsed.words.len() != word_items.len() {
        return Err(anyhow!(
            "Gemini tra {} word segment, trong khi can {} tu.",
            parsed.words.len(),
            word_items.len()
        ));
    }

    let mut out = Vec::with_capacity(word_items.len());
    for (idx, item) in word_items.iter().enumerate() {
        let segment = &parsed.words[idx];
        if segment.line_index != item.line_index + 1 || segment.word_index != item.word_index + 1 {
            return Err(anyhow!(
                "Gemini tra sai thu tu word timing o line {}, word {}.",
                item.line_index + 1,
                item.word_index + 1
            ));
        }
        let start_seconds = segment.start_ms as f64 / 1000.0;
        let mut end_seconds = segment.end_ms as f64 / 1000.0;
        if end_seconds <= start_seconds {
            end_seconds = item.fallback_end_seconds.max(start_seconds + 0.04);
        }
        out.push(TimedWordSegment {
            line_index: item.line_index,
            word_index: item.word_index,
            word: item.word.clone(),
            start_seconds,
            end_seconds,
        });
    }

    Ok(out)
}

fn find_preview_audio_for_srt(srt_path: &Path) -> Option<PathBuf> {
    let root = normalize_project_root(srt_path);
    if let Some(path) = find_latest_audiobook_in_project(&root) {
        return Some(path);
    }
    let candidates = [
        project_audio_dir(&root).join("audiobook.wav"),
        project_audio_dir(&root).join("audiobook.mp3"),
        project_audio_dir(&root).join("audiobook.m4a"),
        project_audio_dir(&root).join("audiobook.flac"),
        project_audio_dir(&root).join("audiobook.ogg"),
        root.join("audiobook.wav"),
        root.join("audiobook.mp3"),
        root.join("audiobook.m4a"),
        root.join("audiobook.flac"),
        root.join("audiobook.ogg"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn parse_srt_timestamp(input: &str) -> Result<f64> {
    let clean = input.trim();
    let parts = clean.split([':', ',']).collect::<Vec<_>>();
    if parts.len() != 4 {
        return Err(anyhow!("Kh?ng dung dinh ?ang SRT"));
    }
    let hours = parts[0].parse::<u64>()?;
    let minutes = parts[1].parse::<u64>()?;
    let seconds = parts[2].parse::<u64>()?;
    let millis = parts[3].parse::<u64>()?;
    Ok(hours as f64 * 3600.0 + minutes as f64 * 60.0 + seconds as f64 + millis as f64 / 1000.0)
}

fn export_video_from_cache(
    lines: &[BookLine],
    speeches: &[ExportedSpeech],
    audiobook_path: &Path,
    output_dir: &Path,
    background_path: Option<&Path>,
    font_name: &str,
    text_color: &str,
    card_opacity: u8,
    font_size: u32,
    corner_tag: &str,
    tag_font_size: u32,
    tag_background_enabled: bool,
    tag_position: VideoTagPosition,
    resolution: VideoResolution,
    frame_rate: VideoFrameRate,
    pause_ms: u32,
    api_key: &str,
    enable_word_highlight: bool,
    cached_timed_lines: &[TimedBookLine],
    cached_word_segments: &[TimedWordSegment],
    cached_srt_path: &str,
    cached_audio_path: &str,
    subtitle_lead_seconds: f64,
    cancel_flag: &AtomicBool,
    progress_tx: Sender<VideoEvent>,
) -> Result<PathBuf> {
    ensure_not_cancelled(cancel_flag, "Video export cancelled.")?;
    ensure_not_cancelled(cancel_flag, "Preview export cancelled.")?;
    if !audiobook_path.exists() {
        return Err(anyhow!("Kh?ng tim thay audiobook de xuat video."));
    }
    if api_key.trim().is_empty() {
        return Err(anyhow!("Thi?u Gemini API key d? t?o SRT cho video."));
    }
    fs::create_dir_all(output_dir)
        .with_context(|| format!("Kh?ng tao duoc {}", output_dir.display()))?;
    ensure_project_structure(output_dir)?;
    let background_path = copy_background_to_project(output_dir, background_path)?;

    let (timed_lines, word_segments, srt_path) = if !cached_timed_lines.is_empty()
        && !cached_srt_path.trim().is_empty()
        && Path::new(cached_srt_path).exists()
        && !cached_audio_path.trim().is_empty()
        && Path::new(cached_audio_path).exists()
    {
        (
            cached_timed_lines.to_vec(),
            cached_word_segments.to_vec(),
            PathBuf::from(cached_srt_path),
        )
    } else {
        let (path, timed_lines, word_segments) = build_video_srt_with_gemini(
            api_key,
            lines,
            speeches,
            audiobook_path,
            output_dir,
            pause_ms,
            enable_word_highlight,
            subtitle_lead_seconds,
            cancel_flag,
            progress_tx.clone(),
        )?;
        (timed_lines, word_segments, path)
    };
    let (width, height) = resolution.dimensions();
    let subtitle_path = next_numbered_file_path(&project_subtitle_dir(output_dir), "captions", "ass")?;
    let video_path = next_numbered_file_path(&project_video_final_dir(output_dir), "video", "mp4")?;
    let ass_content = build_ass_subtitles(
        &timed_lines,
        &word_segments,
        width,
        height,
        font_name,
        text_color,
        card_opacity,
        font_size,
        corner_tag,
        tag_font_size,
        tag_background_enabled,
        tag_position,
    );
    fs::write(&subtitle_path, ass_content)
        .with_context(|| format!("Kh?ng ghi duoc {}", subtitle_path.display()))?;
    let total_duration = timed_lines.last().map(|line| line.end_seconds).unwrap_or(0.0);
    let _ = progress_tx.send(VideoEvent::Progress {
        fraction: 0.30,
        label: format!("?? t?o SRT + subtitle: {}", srt_path.display()),
    });

    let ffmpeg = find_ffmpeg()?;
    let mut command = Command::new(ffmpeg);
    command.arg("-y").args([
        "-progress",
        "pipe:1",
        "-nostats",
        "-filter_threads",
        "1",
        "-filter_complex_threads",
        "1",
    ]);
    if let Some(ref path) = background_path {
        if path.exists() {
            command
                .args(["-loop", "1"])
                .arg("-i")
                .arg(path);
        } else {
            command
                .args(["-f", "lavfi"])
                .arg("-i")
                .arg(format!(
                    "color=c=black:s={}x{}:r={}",
                    width,
                    height,
                    frame_rate.value()
                ));
        }
    } else {
        command
            .args(["-f", "lavfi"])
            .arg("-i")
            .arg(format!(
                "color=c=black:s={}x{}:r={}",
                width,
                height,
                frame_rate.value()
            ));
    }
    command.arg("-i").arg(audiobook_path);
    command.args(["-shortest"]);
    command.args([
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-tune",
        "stillimage",
        "-threads",
        "2",
        "-profile:v",
        "baseline",
        "-pix_fmt",
        "yuv420p",
    ]);
    command.args(["-c:a", "aac", "-b:a", "96k"]);
    let video_filter = if let Some(ref path) = background_path {
        if path.exists() {
            format!(
                "scale={}:{}:force_original_aspect_ratio=increase,crop={}:{},setsar=1,fps={},ass=filename={}",
                width,
                height,
                width,
                height,
                frame_rate.value(),
                escape_ffmpeg_ass_filename(&subtitle_path)
            )
        } else {
            format!(
                "setsar=1,fps={},ass=filename={}",
                frame_rate.value(),
                escape_ffmpeg_ass_filename(&subtitle_path)
            )
        }
    } else {
        format!(
            "setsar=1,fps={},ass=filename={}",
            frame_rate.value(),
            escape_ffmpeg_ass_filename(&subtitle_path)
        )
    };
    command.args([
        "-vf",
        &video_filter,
    ]);
    command.arg(&video_path);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = command.spawn().context("Kh?ng goi duoc ffmpeg de xuat video")?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("Kh?ng doc duoc stdout c?a ffmpeg"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("Kh?ng doc duoc stderr c?a ffmpeg"))?;

    let progress_reader = BufReader::new(stdout);
    let stderr_handle = thread::spawn(move || {
        let mut err = String::new();
        let mut reader = BufReader::new(stderr);
        let _ = std::io::Read::read_to_string(&mut reader, &mut err);
        err
    });

    let mut last_reported = 0.12f32;
    for line in progress_reader.lines() {
        if cancel_flag.load(Ordering::SeqCst) {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stderr_handle.join();
            return Err(anyhow!("Video export cancelled."));
        }
        let line = line.unwrap_or_default();
        if let Some(value) = line.strip_prefix("out_time_us=") {
            if let Ok(out_time_us) = value.trim().parse::<u64>() {
                let seconds = out_time_us as f64 / 1_000_000.0;
                if total_duration > 0.0 {
                    let fraction = (seconds / total_duration).clamp(0.0, 1.0) as f32;
                    if fraction - last_reported >= 0.01 || fraction >= 0.99 {
                        last_reported = fraction;
                        let _ = progress_tx.send(VideoEvent::Progress {
                            fraction,
                            label: format!("?ang encode video... {:>3}%", (fraction * 100.0) as u32),
                        });
                    }
                }
            }
        }
    }

    let status = child.wait().context("Kh?ng doi duoc ffmpeg ket thuc")?;
    let stderr_text = stderr_handle
        .join()
        .unwrap_or_else(|_| String::new());
    if !status.success() {
        return Err(anyhow!("ffmpeg xuat video that bai: {}", stderr_text));
    }
    let _ = progress_tx.send(VideoEvent::Progress {
        fraction: 1.0,
        label: "?? encode xong video.".to_string(),
    });
    Ok(video_path)
}

fn export_video_preview_segment(
    timed_lines: &[TimedBookLine],
    audiobook_path: &Path,
    output_dir: &Path,
    background_path: Option<&Path>,
    font_name: &str,
    text_color: &str,
    card_opacity: u8,
    font_size: u32,
    corner_tag: &str,
    tag_font_size: u32,
    tag_background_enabled: bool,
    tag_position: VideoTagPosition,
    resolution: VideoResolution,
    frame_rate: VideoFrameRate,
    word_segments: &[TimedWordSegment],
    start_seconds: f64,
    duration_seconds: f64,
    cancel_flag: &AtomicBool,
    progress_tx: Sender<VideoEvent>,
) -> Result<PathBuf> {
    ensure_not_cancelled(cancel_flag, "Preview export cancelled.")?;
    if !audiobook_path.exists() {
        return Err(anyhow!("Kh?ng tim thay audiobook de xuat preview video."));
    }
    let requested_end = (start_seconds + duration_seconds.max(0.5)).max(start_seconds + 0.5);
    let (window_start, end_seconds) =
        expanded_preview_window(timed_lines, start_seconds, requested_end, 3);
    let mut clipped_lines = Vec::new();
    for line in timed_lines {
        if line.end_seconds <= window_start || line.start_seconds >= end_seconds {
            continue;
        }
        clipped_lines.push(TimedBookLine {
            speaker: line.speaker.clone(),
            text: line.text.clone(),
            start_seconds: (line.start_seconds - window_start).max(0.0),
            end_seconds: (line.end_seconds.min(end_seconds) - window_start).max(0.12),
        });
    }
    if clipped_lines.is_empty() {
        return Err(anyhow!("?o?n preview ?ang ch?n kh?ng c? subtitle n?o."));
    }
    let mut clipped_word_segments = Vec::new();
    for segment in word_segments {
        if segment.end_seconds <= window_start || segment.start_seconds >= end_seconds {
            continue;
        }
        clipped_word_segments.push(TimedWordSegment {
            line_index: segment.line_index,
            word_index: segment.word_index,
            word: segment.word.clone(),
            start_seconds: (segment.start_seconds - window_start).max(0.0),
            end_seconds: (segment.end_seconds.min(end_seconds) - window_start).max(0.04),
        });
    }

    ensure_project_structure(output_dir)?;
    let background_path = copy_background_to_project(output_dir, background_path)?;
    let (width, height) = resolution.dimensions();
    let subtitle_path = next_numbered_file_path(&project_subtitle_dir(output_dir), "preview_captions", "ass")?;
    let video_path = next_numbered_file_path(&project_video_preview_dir(output_dir), "preview_video", "mp4")?;
    let ass_content = build_ass_subtitles(
        &clipped_lines,
        &clipped_word_segments,
        width,
        height,
        font_name,
        text_color,
        card_opacity,
        font_size,
        corner_tag,
        tag_font_size,
        tag_background_enabled,
        tag_position,
    );
    fs::write(&subtitle_path, ass_content)
        .with_context(|| format!("Kh?ng ghi duoc {}", subtitle_path.display()))?;
    let _ = progress_tx.send(VideoEvent::Progress {
        fraction: 0.20,
        label: "?? d?ng subtitle cho preview clip...".to_string(),
    });

    let ffmpeg = find_ffmpeg()?;
    let mut command = Command::new(ffmpeg);
    command.arg("-y").args(["-progress", "pipe:1", "-nostats"]);
    if let Some(ref path) = background_path {
        if path.exists() {
            command
                .args(["-loop", "1"])
                .arg("-i")
                .arg(path);
        } else {
            command
                .args(["-f", "lavfi"])
                .arg("-i")
                .arg(format!(
                    "color=c=black:s={}x{}:r={}",
                    width,
                    height,
                    frame_rate.value()
                ));
        }
    } else {
        command
            .args(["-f", "lavfi"])
            .arg("-i")
            .arg(format!(
                "color=c=black:s={}x{}:r={}",
                width,
                height,
                frame_rate.value()
            ));
    }
    command
        .args([
            "-ss",
            &format!("{:.3}", window_start),
            "-t",
            &format!("{:.3}", (end_seconds - window_start).max(0.5)),
        ])
        .arg("-i")
        .arg(audiobook_path);
    command.args(["-shortest"]);
    command.args([
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-tune",
        "stillimage",
        "-threads",
        "2",
        "-pix_fmt",
        "yuv420p",
    ]);
    command.args(["-c:a", "aac", "-b:a", "96k"]);
    let video_filter = if let Some(ref path) = background_path {
        if path.exists() {
            format!(
                "scale={}:{}:force_original_aspect_ratio=increase,crop={}:{},setsar=1,fps={},ass=filename={}",
                width,
                height,
                width,
                height,
                frame_rate.value(),
                escape_ffmpeg_ass_filename(&subtitle_path)
            )
        } else {
            format!(
                "setsar=1,fps={},ass=filename={}",
                frame_rate.value(),
                escape_ffmpeg_ass_filename(&subtitle_path)
            )
        }
    } else {
        format!(
            "setsar=1,fps={},ass=filename={}",
            frame_rate.value(),
            escape_ffmpeg_ass_filename(&subtitle_path)
        )
    };
    command.args(["-vf", &video_filter]);
    command.arg(&video_path);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = command.spawn().context("Kh?ng goi duoc ffmpeg de xuat preview video")?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("Kh?ng doc duoc stdout c?a ffmpeg preview"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("Kh?ng doc duoc stderr c?a ffmpeg preview"))?;
    let progress_reader = BufReader::new(stdout);
    let stderr_handle = thread::spawn(move || {
        let mut err = String::new();
        let mut reader = BufReader::new(stderr);
        let _ = std::io::Read::read_to_string(&mut reader, &mut err);
        err
    });

    for line in progress_reader.lines() {
        if cancel_flag.load(Ordering::SeqCst) {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stderr_handle.join();
            return Err(anyhow!("Preview export cancelled."));
        }
        let line = line.unwrap_or_default();
        if let Some(value) = line.strip_prefix("out_time_us=") {
            if let Ok(out_time_us) = value.trim().parse::<u64>() {
                let seconds = out_time_us as f64 / 1_000_000.0;
                let preview_duration = (end_seconds - window_start).max(0.5);
                let fraction = (seconds / preview_duration).clamp(0.0, 1.0) as f32;
                let _ = progress_tx.send(VideoEvent::Progress {
                    fraction,
                    label: format!("?ang encode preview... {:>3}%", (fraction * 100.0) as u32),
                });
            }
        }
    }

    let status = child.wait().context("Kh?ng doi duoc ffmpeg preview ket thuc")?;
    let stderr_text = stderr_handle.join().unwrap_or_else(|_| String::new());
    if !status.success() {
        return Err(anyhow!("ffmpeg xuat preview video that bai: {}", stderr_text));
    }
    Ok(video_path)
}

fn expanded_preview_window(
    timed_lines: &[TimedBookLine],
    requested_start: f64,
    requested_end: f64,
    target_lines: usize,
) -> (f64, f64) {
    let start = requested_start.max(0.0);
    let mut end = requested_end.max(start + 0.5);
    let mut overlap_count = timed_lines
        .iter()
        .filter(|line| line.end_seconds > start && line.start_seconds < end)
        .count();

    if overlap_count >= target_lines || timed_lines.is_empty() {
        return (start, end);
    }

    if let Some(mut next_index) = timed_lines
        .iter()
        .position(|line| line.start_seconds >= end)
        .or_else(|| timed_lines.iter().position(|line| line.end_seconds > end))
    {
        while overlap_count < target_lines {
            let Some(line) = timed_lines.get(next_index) else {
                break;
            };
            end = end.max(line.end_seconds);
            overlap_count = timed_lines
                .iter()
                .filter(|item| item.end_seconds > start && item.start_seconds < end)
                .count();
            next_index += 1;
        }
    }

    if overlap_count == 0 {
        if let Some(line) = timed_lines
            .iter()
            .find(|line| line.end_seconds > start)
            .or_else(|| timed_lines.last())
        {
            end = end.max(line.end_seconds);
        }
    }

    (start, end.max(start + 0.5))
}

#[derive(Clone)]
struct SubtitleRenderLine {
    wrapped: String,
    current_height: u32,
    previous_height: u32,
}

#[derive(Clone)]
struct SubtitleStateItem {
    index: usize,
    style_name: &'static str,
    bottom_y: u32,
}

fn subtitle_state_item_height(item: &SubtitleStateItem, rendered: &[SubtitleRenderLine]) -> u32 {
    if item.style_name == "Current" {
        rendered[item.index].current_height
    } else {
        rendered[item.index].previous_height
    }
}

fn build_subtitle_state_layout(
    active_index: usize,
    rendered: &[SubtitleRenderLine],
    card_top: u32,
    card_bottom: u32,
    line_gap: u32,
) -> Vec<SubtitleStateItem> {
    let mut out = Vec::new();
    if rendered.is_empty() || active_index >= rendered.len() {
        return out;
    }

    let inner_top = card_top.saturating_add(line_gap);
    let inner_bottom = card_bottom.saturating_sub(line_gap);
    let inner_height = inner_bottom.saturating_sub(inner_top).max(1);
    let current_height = rendered[active_index].current_height.min(inner_height);
    let current_top = inner_top + inner_height.saturating_sub(current_height) / 2;
    let current_bottom = current_top + current_height;

    if active_index > 0 {
        let previous_bottom = current_top.saturating_sub(line_gap);
        out.push(SubtitleStateItem {
            index: active_index - 1,
            style_name: "Previous",
            bottom_y: previous_bottom,
        });
    }

    out.push(SubtitleStateItem {
        index: active_index,
        style_name: "Current",
        bottom_y: current_bottom,
    });

    if active_index + 1 < rendered.len() {
        let next_height = rendered[active_index + 1].previous_height;
        let next_top = current_bottom.saturating_add(line_gap);
        out.push(SubtitleStateItem {
            index: active_index + 1,
            style_name: "Previous",
            bottom_y: next_top.saturating_add(next_height),
        });
    }
    out
}

fn lerp_f32(start: f32, end: f32, t: f32) -> f32 {
    start + (end - start) * t.clamp(0.0, 1.0)
}

fn lerp_color32(start: egui::Color32, end: egui::Color32, t: f32) -> egui::Color32 {
    let t = t.clamp(0.0, 1.0);
    let lerp = |a: u8, b: u8| -> u8 { ((a as f32) + ((b as f32) - (a as f32)) * t).round() as u8 };
    egui::Color32::from_rgba_unmultiplied(
        lerp(start.r(), end.r()),
        lerp(start.g(), end.g()),
        lerp(start.b(), end.b()),
        lerp(start.a(), end.a()),
    )
}

fn build_ass_subtitles(
    lines: &[TimedBookLine],
    word_segments: &[TimedWordSegment],
    width: u32,
    height: u32,
    font_name: &str,
    text_color: &str,
    card_opacity: u8,
    font_size: u32,
    corner_tag: &str,
    tag_font_size: u32,
    tag_background_enabled: bool,
    tag_position: VideoTagPosition,
) -> String {
    let primary = ass_color_from_hex(text_color, "FFFFFF");
    let faded = ass_color_with_alpha(text_color, "FFFFFF", "88");
    let card_alpha = format!("{:02X}", 255u8.saturating_sub(card_opacity));
    let previous_size = (font_size as f32 * 0.76).round() as u32;
    let x = width / 2;
    let line_gap = (font_size as f32 * 0.42).round() as u32;
    let current_line_height = (font_size as f32 * 1.16).round() as u32;
    let previous_line_height = (previous_size as f32 * 1.12).round() as u32;
    let max_chars = if width >= 1920 { 42 } else { 34 };
    let card_left = (width as f32 * 0.11).round() as u32;
    let card_right = width.saturating_sub(card_left);
    let card_top = (height as f32 * 0.56).round() as u32;
    let card_bottom = (height as f32 * 0.90).round() as u32;
    let tag_size = estimate_tag_square_size(corner_tag, tag_font_size);
    let tag_left = match tag_position {
        VideoTagPosition::TopLeft => 0,
        VideoTagPosition::TopCenter => width.saturating_sub(tag_size) / 2,
    };
    let tag_top = 0;
    let tag_right = tag_left + tag_size;
    let tag_bottom = tag_top + tag_size;

    let rendered = lines
        .iter()
        .map(|line| {
            let wrapped = wrap_preview_text(&display_subtitle_text(&line.speaker, &line.text), max_chars);
            let line_count = wrapped.lines().count().max(1) as u32;
            SubtitleRenderLine {
                wrapped,
                current_height: line_count.saturating_mul(current_line_height),
                previous_height: line_count.saturating_mul(previous_line_height),
            }
        })
        .collect::<Vec<_>>();

    let mut out = String::new();
    out.push_str("[Script Info]\n");
    out.push_str("ScriptType: v4.00+\n");
    out.push_str(&format!("PlayResX: {}\nPlayResY: {}\n", width, height));
    out.push_str("WrapStyle: 2\nScaledBorderAndShadow: yes\n\n");
    out.push_str("[V4+ Styles]\n");
    out.push_str("Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n");
    out.push_str(&format!(
        "Style: Current,{},{},{},&H000000FF,&H64000000,&H82000000,-1,0,0,0,100,100,0,0,1,2,1,2,40,40,40,1\n",
        font_name,
        font_size,
        primary
    ));
    out.push_str(&format!(
        "Style: Previous,{},{},{},&H000000FF,&H64000000,&H82000000,0,0,0,0,100,100,0,0,1,2,1,2,40,40,40,1\n\n",
        font_name,
        previous_size.max(18),
        faded
    ));
    out.push_str(&format!(
        "Style: TagText,{},{},{},&H000000FF,&H64000000,&H82000000,-1,0,0,0,100,100,0,0,1,2,1,5,20,20,20,1\n",
        font_name,
        tag_font_size.clamp(24, 120),
        primary
    ));
    out.push_str("Style: Card,Tahoma,20,&H00FFFFFF,&H00FFFFFF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,0,0,7,0,0,0,1\n\n");
    out.push_str("[Events]\n");
    out.push_str("Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n");

    if let Some(last) = lines.last() {
        let full_end = format_ass_time(last.end_seconds.max(0.1));
        let radius = ((font_size as f32) * 0.48).round() as u32;
        out.push_str(&format!(
            "Dialogue: 0,0:00:00.00,{},Card,,0,0,0,,{{\\p1\\bord0\\shad0\\1c&H000000&\\1a&H{}&}}{}\n",
            full_end,
            card_alpha,
            build_ass_rounded_rect_path(card_left, card_top, card_right, card_bottom, radius)
        ));
        if !corner_tag.trim().is_empty() {
            if tag_background_enabled {
                out.push_str(&format!(
                    "Dialogue: 0,0:00:00.00,{},Card,,0,0,0,,{{\\p1\\bord0\\shad0\\1c&H000000&\\1a&H{}&}}{}\n",
                    full_end,
                    card_alpha,
                    build_ass_rect_path(tag_left, tag_top, tag_right, tag_bottom)
                ));
            }
            out.push_str(&format!(
                "Dialogue: 5,0:00:00.00,{},TagText,,0,0,0,,{{\\an7\\pos({},{})}}{}\n",
                full_end,
                tag_left + 14,
                tag_top + 14,
                escape_ass_text(corner_tag.trim())
            ));
        }
    }

    let states = (0..lines.len())
        .map(|idx| {
            build_subtitle_state_layout(
                idx,
                &rendered,
                card_top.saturating_add(line_gap),
                card_bottom.saturating_sub(line_gap),
                line_gap,
            )
        })
        .collect::<Vec<_>>();

    for idx in 0..lines.len() {
        let line = &lines[idx];
        let state = &states[idx];
        let state_start = line.start_seconds;
        let state_end = if idx + 1 < lines.len() {
            lines[idx + 1].start_seconds.max(state_start + 0.05)
        } else {
            line.end_seconds.max(state_start + 0.1)
        };
        let transition_seconds = if idx == 0 {
            0.0
        } else {
            ((line.end_seconds - line.start_seconds).max(0.18) * 0.18)
                .clamp(0.10, 0.22)
                .min((state_end - state_start).max(0.10))
        };
        let hold_start = if idx == 0 {
            state_start
        } else {
            (state_start + transition_seconds).min(state_end)
        };

        let clip_top = state
            .first()
            .map(|item| item.bottom_y.saturating_sub(subtitle_state_item_height(item, &rendered)))
            .unwrap_or(card_top.saturating_add(line_gap))
            .saturating_sub(line_gap);
        let clip_top = clip_top.max(card_top.saturating_add(line_gap));
        let clip_bottom = card_bottom.saturating_sub(line_gap);
        let clip_tag = format!("\\clip({},{},{},{})", card_left, card_top, card_right, card_bottom);

        if idx > 0 && transition_seconds > 0.0 {
            let prev_state = &states[idx - 1];
            let transition_start = format_ass_time(state_start);
            let transition_end = format_ass_time((state_start + transition_seconds).min(state_end));
            let transition_ms = (transition_seconds * 1000.0).round() as u32;

            let mut union_indices = std::collections::BTreeSet::new();
            for item in prev_state {
                union_indices.insert(item.index);
            }
            for item in state {
                union_indices.insert(item.index);
            }

            let prev_clip_top = prev_state
                .first()
                .map(|item| item.bottom_y.saturating_sub(subtitle_state_item_height(item, &rendered)))
                .unwrap_or(clip_top)
                .saturating_sub(line_gap);
            let effective_clip_top = clip_top.min(prev_clip_top);
            let clip_tag_transition = format!(
                "\\clip({},{},{},{})",
                card_left,
                effective_clip_top.max(card_top),
                card_right,
                clip_bottom
            );

            for line_index in union_indices {
                let start_item = prev_state.iter().find(|item| item.index == line_index);
                let end_item = state.iter().find(|item| item.index == line_index);
                let text = rendered[line_index].wrapped.replace('\n', "\\N");
                let start_bottom = if let Some(item) = start_item {
                    item.bottom_y as i32
                } else if end_item.is_some() {
                    clip_bottom as i32
                        + rendered[line_index].current_height as i32
                        + line_gap as i32
                } else {
                    continue;
                };
                let end_bottom = if let Some(item) = end_item {
                    item.bottom_y as i32
                } else {
                    effective_clip_top as i32
                        - rendered[line_index].previous_height as i32
                        - line_gap as i32
                };
                let start_is_current = start_item.is_some_and(|item| item.style_name == "Current");
                let end_is_current = end_item.is_some_and(|item| item.style_name == "Current");
                let base_style = if start_is_current { "Current" } else { "Previous" };
                let start_font_size = if start_is_current {
                    font_size
                } else {
                    previous_size.max(18)
                };
                let end_font_size = if end_is_current {
                    font_size
                } else {
                    previous_size.max(18)
                };
                let start_color = if start_is_current { &primary } else { &faded };
                let end_color = if end_is_current { &primary } else { &faded };
                let start_bold = if start_is_current { 1 } else { 0 };
                let end_bold = if end_is_current { 1 } else { 0 };
                out.push_str(&format!(
                    "Dialogue: 0,{},{},{},,0,0,0,,{{{}\\fs{}\\1c{}\\b{}\\move({},{},{},{},0,{})\\t(0,{},\\fs{}\\1c{}\\b{})}}{}\n",
                    transition_start,
                    transition_end,
                    base_style,
                    clip_tag_transition,
                    start_font_size,
                    start_color,
                    start_bold,
                    x,
                    start_bottom,
                    x,
                    end_bottom,
                    transition_ms.max(1),
                    transition_ms.max(1),
                    end_font_size,
                    end_color,
                    end_bold,
                    escape_ass_text(&text)
                ));
            }
        }

        if hold_start < state_end {
            let hold_start_tag = format_ass_time(hold_start);
            let hold_end_tag = format_ass_time(state_end);
            for item in state {
                if item.style_name == "Current" {
                    let line_words = word_segments_for_line(word_segments, item.index);
                    if line_words.is_empty() {
                        let text = rendered[item.index].wrapped.replace('\n', "\\N");
                        out.push_str(&format!(
                            "Dialogue: 0,{},{},{},,0,0,0,,{{{}\\pos({},{})}}{}\n",
                            hold_start_tag,
                            hold_end_tag,
                            item.style_name,
                            clip_tag,
                            x,
                            item.bottom_y,
                            escape_ass_text(&text)
                        ));
                    } else {
                        let mut segment_start = hold_start;
                        for word in line_words {
                            let overlap_start = word.start_seconds.max(hold_start);
                            let overlap_end = word.end_seconds.min(state_end);
                            if overlap_end <= overlap_start {
                                continue;
                            }
                            if overlap_start > segment_start {
                                out.push_str(&format!(
                                    "Dialogue: 0,{},{},{},,0,0,0,,{{{}\\pos({},{})}}{}\n",
                                    format_ass_time(segment_start),
                                    format_ass_time(overlap_start),
                                    item.style_name,
                                    clip_tag,
                                    x,
                                    item.bottom_y,
                                    build_ass_current_line_text(&lines[item.index], None)
                                ));
                            }
                            out.push_str(&format!(
                                "Dialogue: 0,{},{},{},,0,0,0,,{{{}\\pos({},{})}}{}\n",
                                format_ass_time(overlap_start),
                                format_ass_time(overlap_end),
                                item.style_name,
                                clip_tag,
                                x,
                                item.bottom_y,
                                build_ass_current_line_text(&lines[item.index], Some(word.word_index))
                            ));
                            segment_start = overlap_end;
                        }
                        if segment_start < state_end {
                            out.push_str(&format!(
                                "Dialogue: 0,{},{},{},,0,0,0,,{{{}\\pos({},{})}}{}\n",
                                format_ass_time(segment_start),
                                hold_end_tag,
                                item.style_name,
                                clip_tag,
                                x,
                                item.bottom_y,
                                build_ass_current_line_text(&lines[item.index], None)
                            ));
                        }
                    }
                } else {
                    let text = rendered[item.index].wrapped.replace('\n', "\\N");
                    out.push_str(&format!(
                        "Dialogue: 0,{},{},{},,0,0,0,,{{{}\\pos({},{})}}{}\n",
                        hold_start_tag,
                        hold_end_tag,
                        item.style_name,
                        clip_tag,
                        x,
                        item.bottom_y,
                        escape_ass_text(&text)
                    ));
                }
            }
        }
    }

    out
}

fn format_ass_time(seconds: f64) -> String {
    let total_centis = (seconds.max(0.0) * 100.0).round() as u64;
    let hours = total_centis / 360000;
    let minutes = (total_centis % 360000) / 6000;
    let secs = (total_centis % 6000) / 100;
    let centis = total_centis % 100;
    format!("{}:{:02}:{:02}.{:02}", hours, minutes, secs, centis)
}

fn ass_color_from_hex(hex: &str, fallback: &str) -> String {
    let rgb = normalize_hex_color(hex).unwrap_or_else(|| fallback.to_string());
    let r = &rgb[0..2];
    let g = &rgb[2..4];
    let b = &rgb[4..6];
    format!("&H00{}{}{}", b, g, r)
}

fn ass_color_with_alpha(hex: &str, fallback: &str, alpha: &str) -> String {
    let rgb = normalize_hex_color(hex).unwrap_or_else(|| fallback.to_string());
    let r = &rgb[0..2];
    let g = &rgb[2..4];
    let b = &rgb[4..6];
    format!("&H{}{}{}{}", alpha, b, g, r)
}

fn normalize_hex_color(input: &str) -> Option<String> {
    let trimmed = input.trim().trim_start_matches('#');
    if trimmed.len() != 6 || !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return None;
    }
    Some(trimmed.to_ascii_uppercase())
}

fn parse_hex_color(input: &str) -> Option<egui::Color32> {
    let hex = normalize_hex_color(input)?;
    let r = u8::from_str_radix(&hex[0..2], 16).ok()?;
    let g = u8::from_str_radix(&hex[2..4], 16).ok()?;
    let b = u8::from_str_radix(&hex[4..6], 16).ok()?;
    Some(egui::Color32::from_rgb(r, g, b))
}

fn build_ass_rounded_rect_path(left: u32, top: u32, right: u32, bottom: u32, radius: u32) -> String {
    let radius = radius
        .min((right.saturating_sub(left)) / 2)
        .min((bottom.saturating_sub(top)) / 2)
        .max(2);
    let k = ((radius as f32) * 0.5523).round() as i32;
    let l = left as i32;
    let t = top as i32;
    let r = right as i32;
    let b = bottom as i32;
    let rad = radius as i32;

    format!(
        "m {} {} l {} {} b {} {} {} {} {} {} l {} {} b {} {} {} {} {} {} l {} {} b {} {} {} {} {} {} l {} {} b {} {} {} {} {} {}",
        l + rad, t,
        r - rad, t,
        r - rad + k, t, r, t + rad - k, r, t + rad,
        r, b - rad,
        r, b - rad + k, r - rad + k, b, r - rad, b,
        l + rad, b,
        l + rad - k, b, l, b - rad + k, l, b - rad,
        l, t + rad,
        l, t + rad - k, l + rad - k, t, l + rad, t
    )
}

fn build_ass_rect_path(left: u32, top: u32, right: u32, bottom: u32) -> String {
    format!(
        "m {} {} l {} {} l {} {} l {} {} l {} {}",
        left, top, right, top, right, bottom, left, bottom, left, top
    )
}

fn estimate_tag_square_size(tag_text: &str, font_size: u32) -> u32 {
    let text = tag_text.trim();
    let char_count = text.chars().count().max(1) as f32;
    let estimated_width = char_count * font_size as f32 * 0.42;
    let padded = estimated_width + font_size as f32 * 0.9;
    padded.round().clamp(110.0, 300.0) as u32
}

fn word_segments_for_line<'a>(
    word_segments: &'a [TimedWordSegment],
    line_index: usize,
) -> Vec<&'a TimedWordSegment> {
    word_segments
        .iter()
        .filter(|segment| segment.line_index == line_index)
        .collect()
}

fn active_word_index_for_line(
    word_segments: &[TimedWordSegment],
    line_index: usize,
    elapsed_seconds: f64,
) -> Option<usize> {
    word_segments
        .iter()
        .find(|segment| {
            segment.line_index == line_index
                && elapsed_seconds >= segment.start_seconds
                && elapsed_seconds < segment.end_seconds
        })
        .map(|segment| segment.word_index)
}

fn build_ass_current_line_text(
    line: &TimedBookLine,
    active_word_index: Option<usize>,
) -> String {
    let mut out = escape_ass_text(&subtitle_prefix(&line.speaker));
    let words = split_text_into_words(&line.text);
    if words.is_empty() {
        out.push_str(&escape_ass_text(&line.text));
        return out;
    }
    for (idx, word) in words.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        if Some(idx) == active_word_index {
            out.push_str(r"{\1c&H66D9FF&\b1}");
            out.push_str(&escape_ass_text(word));
            out.push_str(r"{\rCurrent}");
        } else {
            out.push_str(&escape_ass_text(word));
        }
    }
    out
}

fn build_preview_layout_job(
    line: &TimedBookLine,
    active_word_index: Option<usize>,
    font_id: egui::FontId,
    base_color: egui::Color32,
    highlight_color: egui::Color32,
    wrap_width: f32,
    align_center: bool,
) -> egui::text::LayoutJob {
    let mut job = egui::text::LayoutJob::default();
    job.wrap.max_width = wrap_width;
    if align_center {
        job.halign = egui::Align::Center;
    }

    let prefix = subtitle_prefix(&line.speaker);
    if !prefix.is_empty() {
        job.append(
            &prefix,
            0.0,
            egui::TextFormat {
                font_id: font_id.clone(),
                color: base_color,
                ..Default::default()
            },
        );
    }

    let words = split_text_into_words(&line.text);
    if words.is_empty() {
        job.append(
            &line.text,
            0.0,
            egui::TextFormat {
                font_id,
                color: base_color,
                ..Default::default()
            },
        );
        return job;
    }

    for (idx, word) in words.iter().enumerate() {
        let text = if idx + 1 == words.len() {
            word.clone()
        } else {
            format!("{} ", word)
        };
        let mut format = egui::TextFormat {
            font_id: font_id.clone(),
            color: base_color,
            ..Default::default()
        };
        if Some(idx) == active_word_index {
            format.color = highlight_color;
            format.background = egui::Color32::from_rgba_unmultiplied(255, 217, 102, 40);
        }
        job.append(&text, 0.0, format);
    }

    job
}

fn trimmed_path(input: &str) -> Option<PathBuf> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}

fn wrap_preview_text(text: &str, max_chars: usize) -> String {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if current.is_empty() {
            current.push_str(word);
            continue;
        }
        if current.len() + 1 + word.len() > max_chars {
            lines.push(current);
            current = word.to_string();
        } else {
            current.push(' ');
            current.push_str(word);
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines.join("\n")
}

fn escape_ass_text(text: &str) -> String {
    text.replace('{', r"\{").replace('}', r"\}")
}

fn escape_ffmpeg_ass_filename(path: &Path) -> String {
    let value = path
        .to_string_lossy()
        .replace('\\', "/")
        .replace(':', "\\:")
        .replace('\'', "\\'");
    format!("'{}'", value)
}

fn find_ffmpeg() -> Result<PathBuf> {
    let output = Command::new("where")
        .arg("ffmpeg")
        .output()
        .context("Kh?ng goi duoc lenh where ffmpeg")?;
    if !output.status.success() {
        return Err(anyhow!("Kh?ng tim thay ffmpeg trong PATH."));
    }
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let first = stdout
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .ok_or_else(|| anyhow!("Kh?ng tim thay duong ??n ffmpeg."))?;
    Ok(PathBuf::from(first))
}

fn ensure_sentence_like_text(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.ends_with(['.', '!', '?']) {
        trimmed.to_string()
    } else if let Some(stripped) = trimmed.strip_suffix(',') {
        format!("{}.", stripped.trim_end())
    } else if let Some(stripped) = trimmed.strip_suffix(';') {
        format!("{}.", stripped.trim_end())
    } else if let Some(stripped) = trimmed.strip_suffix(':') {
        format!("{}.", stripped.trim_end())
    } else {
        format!("{trimmed}.")
    }
}

fn build_grouped_text(lines: &[BookLine]) -> String {
    lines.iter()
        .map(|line| ensure_sentence_like_text(&line.text))
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn split_grouped_tasks_into_batches(tasks: &[ExportTask], max_chars: usize) -> Vec<Vec<ExportTask>> {
    let mut batches: Vec<Vec<ExportTask>> = Vec::new();
    let mut current: Vec<ExportTask> = Vec::new();
    let mut current_chars = 0usize;

    for task in tasks {
        let line_chars = ensure_sentence_like_text(&task.line.text).chars().count().max(1);
        let separator_chars = if current.is_empty() { 0 } else { 2 };
        if !current.is_empty() && current_chars + separator_chars + line_chars > max_chars {
            batches.push(current);
            current = Vec::new();
            current_chars = 0;
        }
        current_chars += separator_chars + line_chars;
        current.push(task.clone());
    }

    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

fn split_samples_for_grouped_render(samples: &[i16], texts: &[String]) -> Vec<Vec<i16>> {
    if texts.is_empty() {
        return Vec::new();
    }
    if texts.len() == 1 {
        return vec![trim_trailing_silence(samples)];
    }

    let boundaries = detect_group_split_boundaries(samples, texts.len() - 1)
        .unwrap_or_else(|| proportional_group_split_boundaries(samples.len(), texts));
    let mut out = Vec::new();
    let mut start = 0usize;
    for boundary in boundaries {
        let end = boundary.min(samples.len()).max(start);
        out.push(trim_trailing_silence(&samples[start..end]));
        start = end;
    }
    out.push(trim_trailing_silence(&samples[start..]));
    while out.len() < texts.len() {
        out.push(Vec::new());
    }
    out.truncate(texts.len());
    out
}

fn detect_group_split_boundaries(samples: &[i16], needed: usize) -> Option<Vec<usize>> {
    if needed == 0 || samples.len() < SAMPLE_RATE as usize / 2 {
        return None;
    }
    let threshold = 380i32;
    let min_run = (SAMPLE_RATE as usize * 18) / 100;
    let mut runs: Vec<(usize, usize)> = Vec::new();
    let mut run_start: Option<usize> = None;

    for (idx, sample) in samples.iter().enumerate() {
        if i32::from(*sample).abs() <= threshold {
            run_start.get_or_insert(idx);
        } else if let Some(start) = run_start.take() {
            if idx.saturating_sub(start) >= min_run {
                runs.push((start, idx));
            }
        }
    }
    if let Some(start) = run_start {
        if samples.len().saturating_sub(start) >= min_run {
            runs.push((start, samples.len()));
        }
    }
    if runs.is_empty() {
        return None;
    }

    let mut selected = Vec::new();
    let mut used = vec![false; runs.len()];
    for split_index in 1..=needed {
        let target = samples.len() * split_index / (needed + 1);
        let mut best_idx = None;
        let mut best_dist = usize::MAX;
        for (idx, (start, end)) in runs.iter().enumerate() {
            if used[idx] {
                continue;
            }
            let midpoint = (start + end) / 2;
            let dist = midpoint.abs_diff(target);
            if dist < best_dist {
                best_dist = dist;
                best_idx = Some(idx);
            }
        }
        let Some(best_idx) = best_idx else {
            break;
        };
        used[best_idx] = true;
        selected.push((runs[best_idx].0 + runs[best_idx].1) / 2);
    }
    if selected.len() != needed {
        return None;
    }
    selected.sort_unstable();
    Some(selected)
}

fn proportional_group_split_boundaries(total_samples: usize, texts: &[String]) -> Vec<usize> {
    let weights: Vec<usize> = texts
        .iter()
        .map(|text| text.split_whitespace().count().max(1))
        .collect();
    let total_weight: usize = weights.iter().sum();
    let mut consumed_weight = 0usize;
    let mut boundaries = Vec::new();
    for weight in weights.iter().take(weights.len().saturating_sub(1)) {
        consumed_weight += *weight;
        boundaries.push(total_samples * consumed_weight / total_weight.max(1));
    }
    boundaries
}

fn apply_volume_percent(samples: &[i16], gain_percent: u32) -> Vec<i16> {
    if gain_percent == 100 {
        return samples.to_vec();
    }
    let gain = gain_percent as f32 / 100.0;
    samples
        .iter()
        .map(|sample| {
            let scaled = (*sample as f32 * gain).round();
            scaled.clamp(i16::MIN as f32, i16::MAX as f32) as i16
        })
        .collect()
}

fn read_wav_samples(path: &Path) -> Result<Vec<i16>> {
    let mut reader = hound::WavReader::open(path)
        .with_context(|| format!("Kh?ng mo duoc WAV {}", path.display()))?;
    let spec = reader.spec();
    if spec.bits_per_sample != 16 {
        return Err(anyhow!(
            "WAV {} khong phai 16-bit PCM.",
            path.display()
        ));
    }
    let mut out = Vec::new();
    for sample in reader.samples::<i16>() {
        out.push(sample.with_context(|| format!("Loi doc sample {}", path.display()))?);
    }
    Ok(out)
}

fn transcribe_audio_with_gemini(
    api_key: &str,
    audio_path: &Path,
    language_hint: Option<&str>,
) -> Result<String> {
    validate_api_key(api_key)?;
    let mime_type = infer_audio_mime_type(audio_path)
        .ok_or_else(|| anyhow!("Dinh ?ang audio khong duoc ho tro."))?;
    let bytes = fs::read(audio_path)
        .with_context(|| format!("Kh?ng doc duoc file {}", audio_path.display()))?;
    if bytes.len() > MAX_INLINE_AUDIO_BYTES {
        return Err(anyhow!(
            "File qua lon de gui inline. Hay dung clip duoi {} MB.",
            MAX_INLINE_AUDIO_BYTES / (1024 * 1024)
        ));
    }

    let language_prefix = match language_hint {
        Some(language) if !language.trim().is_empty() && language != "Auto" => {
            format!("The spoken language is {}. ", language)
        }
        _ => String::new(),
    };

    let payload = serde_json::json!({
        "contents": [{
            "parts": [
                {
                    "text": format!(
                        "{}Transcribe this audio exactly. Return only the transcript text. No quotes, no labels, no explanation.",
                        language_prefix
                    )
                },
                {
                    "inline_data": {
                        "mime_type": mime_type,
                        "data": general_purpose::STANDARD.encode(bytes),
                    }
                }
            ]
        }],
        "generationConfig": {
            "temperature": 0
        }
    });

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        ANALYSIS_MODEL, api_key
    );

    let response = ureq::post(&url)
        .content_type("application/json")
        .send(&payload.to_string())
        .map_err(|err| anyhow!("Kh?ng goi duoc Gemini tach chu: {}", err))?;
    let body = response
        .into_body()
        .read_to_string()
        .map_err(|err| anyhow!("Kh?ng doc duoc phan hoi Gemini: {}", err))?;
    let json: Value =
        serde_json::from_str(&body).map_err(|err| anyhow!("JSON Gemini khong hop le: {}", err))?;

    extract_candidate_text(&json)
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("Gemini khong tra transcript hop le."))
}

fn qwen_service_generate(
    base_url: &str,
    ref_audio_path: &str,
    ref_text: &str,
    target_text: &str,
    language: &str,
    xvector_only: bool,
    output_path: &Path,
) -> Result<QwenGenerateResponse> {
    let payload = serde_json::json!({
        "ref_audio": ref_audio_path,
        "ref_text": ref_text,
        "target_text": target_text,
        "language": language,
        "xvector_only": xvector_only,
        "output_path": output_path.to_string_lossy().to_string(),
    });

    let url = format!("{}/generate", base_url);
    let response = ureq::post(&url)
        .config()
        .timeout_connect(Some(Duration::from_secs(8)))
        .timeout_global(Some(Duration::from_secs(QWEN_REQUEST_TIMEOUT_SECS)))
        .timeout_recv_response(Some(Duration::from_secs(QWEN_REQUEST_TIMEOUT_SECS)))
        .timeout_recv_body(Some(Duration::from_secs(QWEN_REQUEST_TIMEOUT_SECS)))
        .build()
        .content_type("application/json")
        .send(&payload.to_string())
        .map_err(|err| anyhow!("Kh?ng goi duoc Qwen Service {}: {}", base_url, err))?;
    let body = response
        .into_body()
        .read_to_string()
        .map_err(|err| anyhow!("Kh?ng doc duoc phan hoi Qwen Service: {}", err))?;
    let json: Value = serde_json::from_str(&body)
        .map_err(|err| anyhow!("JSON Qwen Service khong hop le: {}", err))?;

    if json.get("ok").and_then(Value::as_bool) == Some(false) {
        return Err(anyhow!(
            "{}",
            json.get("error")
                .and_then(Value::as_str)
                .unwrap_or("Qwen Service loi.")
        ));
    }

    serde_json::from_value(json)
        .map_err(|err| anyhow!("Kh?ng parse duoc ket qua Qwen Service: {}", err))
}

fn qwen_service_generate_retry(
    base_url: &str,
    ref_audio_path: &str,
    ref_text: &str,
    target_text: &str,
    language: &str,
    xvector_only: bool,
    output_path: &Path,
) -> Result<QwenGenerateResponse> {
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 0..=QWEN_SERVICE_RETRIES {
        match qwen_service_generate(
            base_url,
            ref_audio_path,
            ref_text,
            target_text,
            language,
            xvector_only,
            output_path,
        ) {
            Ok(result) => return Ok(result),
            Err(err) => {
                let err_text = err.to_string();
                if err_text.contains("Connection refused") || err_text.contains("connection refused")
                {
                    let _ = ensure_qwen_service_background(base_url);
                    thread::sleep(Duration::from_secs(2));
                } else {
                    thread::sleep(Duration::from_millis(400));
                }
                last_error = Some(anyhow!(
                    "Qwen request failed on attempt {}: {}",
                    attempt + 1,
                    err_text
                ));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("Kh?ng goi duoc Qwen Service.")))
}

fn qwen_service_generate_with_chunking(
    base_url: &str,
    ref_audio_path: &str,
    ref_text: &str,
    target_text: &str,
    language: &str,
    xvector_only: bool,
    temp_dir: &Path,
) -> Result<QwenChunkedResult> {
    fs::create_dir_all(temp_dir)
        .with_context(|| format!("Kh?ng tao duoc thu muc {}", temp_dir.display()))?;

    let mut resolved_ref_text = ref_text.to_string();
    let full_file = temp_dir.join("full.wav");
    let normalized_target_text = ensure_sentence_like_text(target_text);

    if let Ok(result) = qwen_service_generate_retry(
        base_url,
        ref_audio_path,
        &resolved_ref_text,
        &normalized_target_text,
        language,
        xvector_only,
        &full_file,
    ) {
        if resolved_ref_text.trim().is_empty() && !result.ref_text.trim().is_empty() {
            resolved_ref_text = result.ref_text.clone();
        }
        let samples = read_wav_samples(Path::new(&result.output_path))?;
        let _ = fs::remove_file(&full_file);
        return Ok(QwenChunkedResult {
            samples,
            ref_text: resolved_ref_text,
        });
    }

    let segments = split_tts_segments(&normalized_target_text);
    let mut all_samples = Vec::new();

    for (index, segment) in segments.iter().enumerate() {
        let temp_file = temp_dir.join(format!("chunk_{index:03}.wav"));
        let normalized_segment = ensure_sentence_like_text(segment);
        let result = qwen_service_generate_retry(
            base_url,
            ref_audio_path,
            &resolved_ref_text,
            &normalized_segment,
            language,
            xvector_only,
            &temp_file,
        )
        .or_else(|_| {
            qwen_service_generate_retry(
                base_url,
                ref_audio_path,
                &resolved_ref_text,
                &normalized_segment,
                language,
                xvector_only,
                &temp_file,
            )
        })?;
        if resolved_ref_text.trim().is_empty() && !result.ref_text.trim().is_empty() {
            resolved_ref_text = result.ref_text.clone();
        }
        let samples = read_wav_samples(Path::new(&result.output_path))?;
        append_samples_with_pause(&mut all_samples, &samples, index > 0, 140);
        let _ = fs::remove_file(&temp_file);
    }

    Ok(QwenChunkedResult {
        samples: all_samples,
        ref_text: resolved_ref_text,
    })
}

fn storage_root() -> PathBuf {
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            if exe_dir.join("Cargo.toml").exists() {
                return exe_dir.to_path_buf();
            }
            if let Some(target_dir) = exe_dir.parent() {
                if let Some(project_dir) = target_dir.parent() {
                    if project_dir.join("Cargo.toml").exists() {
                        return project_dir.to_path_buf();
                    }
                }
            }
        }
    }
    fallback
}

fn app_data_dir() -> PathBuf {
    storage_root().join(APP_DATA_DIR)
}

fn persisted_state_path() -> PathBuf {
    app_data_dir().join("state.json")
}

fn load_persisted_state() -> Result<PersistedState> {
    let path = persisted_state_path();
    if !path.exists() {
        return Ok(PersistedState::default());
    }
    let content =
        fs::read_to_string(&path).with_context(|| format!("Kh?ng doc duoc {}", path.display()))?;
    let state = serde_json::from_str::<PersistedState>(&content)
        .with_context(|| format!("JSON state khong hop le: {}", path.display()))?;
    Ok(state)
}

fn save_persisted_state(state: &PersistedState) -> Result<()> {
    let dir = app_data_dir();
    fs::create_dir_all(&dir).with_context(|| format!("Kh?ng tao duoc {}", dir.display()))?;
    let path = persisted_state_path();
    let content = serde_json::to_string_pretty(state).context("Kh?ng serialize duoc state")?;
    fs::write(&path, content).with_context(|| format!("Kh?ng ghi duoc {}", path.display()))?;
    Ok(())
}

fn app_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn qwen_restart_guard() -> &'static Mutex<Option<Instant>> {
    static GUARD: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(None))
}

fn launch_qwen_service_process(port: u16, hidden: bool) -> Result<()> {
    let root = storage_root();
    let python_path = root.join("engines").join("qwen3tts").join(".venv").join("Scripts").join("python.exe");
    let script_path = root.join("qwen_service.py");
    if !python_path.exists() {
        return Err(anyhow!("Kh?ng tim thay Python Qwen {}", python_path.display()));
    }
    if !script_path.exists() {
        return Err(anyhow!("Kh?ng tim thay qwen_service.py {}", script_path.display()));
    }

    let cache_root = root.join("models-cache");
    let hf_home = cache_root.join("huggingface");
    let hf_hub_cache = hf_home.join("hub");
    let hf_assets_cache = hf_home.join("assets");
    let hf_datasets_cache = hf_home.join("datasets");
    let torch_home = cache_root.join("torch");
    let pip_cache = cache_root.join("pip");
    let triton_cache = cache_root.join("triton");
    let cuda_cache = cache_root.join("cuda");
    let torchinductor_cache = cache_root.join("torchinductor");
    let pycache = cache_root.join("pycache");
    let numba_cache = cache_root.join("numba");
    let gradio_temp = cache_root.join("gradio");
    let temp_dir = cache_root.join("tmp");

    for dir in [
        &hf_hub_cache,
        &hf_assets_cache,
        &hf_datasets_cache,
        &torch_home,
        &pip_cache,
        &triton_cache,
        &cuda_cache,
        &torchinductor_cache,
        &pycache,
        &numba_cache,
        &gradio_temp,
        &temp_dir,
    ] {
        let _ = fs::create_dir_all(dir);
    }

    let mut command = Command::new(python_path);
    command
        .current_dir(&root)
        .arg(script_path)
        .args(["--host", "127.0.0.1", "--port", &port.to_string(), "--device", "cuda:0", "--dtype", "float16"])
        .env("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")
        .env("HF_HUB_DISABLE_XET", "1")
        .env("PYTHONIOENCODING", "utf-8")
        .env("HF_HOME", &hf_home)
        .env("HF_HUB_CACHE", &hf_hub_cache)
        .env("HF_ASSETS_CACHE", &hf_assets_cache)
        .env("HF_DATASETS_CACHE", &hf_datasets_cache)
        .env("HUGGINGFACE_HUB_CACHE", &hf_hub_cache)
        .env("HUGGINGFACE_ASSETS_CACHE", &hf_assets_cache)
        .env("TORCH_HOME", &torch_home)
        .env("PIP_CACHE_DIR", &pip_cache)
        .env("TRITON_CACHE_DIR", &triton_cache)
        .env("CUDA_CACHE_PATH", &cuda_cache)
        .env("TORCHINDUCTOR_CACHE_DIR", &torchinductor_cache)
        .env("PYTHONPYCACHEPREFIX", &pycache)
        .env("NUMBA_CACHE_DIR", &numba_cache)
        .env("GRADIO_TEMP_DIR", &gradio_temp)
        .env("XDG_CACHE_HOME", &cache_root)
        .env("TEMP", &temp_dir)
        .env("TMP", &temp_dir)
        .env_remove("TRANSFORMERS_CACHE")
        .env_remove("PYTORCH_TRANSFORMERS_CACHE")
        .env_remove("PYTORCH_PRETRAINED_BERT_CACHE");
    #[cfg(target_os = "windows")]
    {
        if hidden {
            command.creation_flags(0x08000000);
        } else {
            command.creation_flags(0x00000010);
        }
    }
    command
        .spawn()
        .with_context(|| format!("Kh?ng mo duoc Qwen Service port {}", port))?;

    Ok(())
}

fn launch_batch_in_console_with_args(batch_name: &str, args: &[&str]) -> Result<()> {
    let root = app_root();
    let batch_path = root.join(batch_name);
    if !batch_path.exists() {
        return Err(anyhow!("Kh?ng tim thay file {}", batch_path.display()));
    }

    let mut command = Command::new("cmd");
    command
        .current_dir(&root)
        .args(["/C", "start", "", "cmd", "/K", "call"])
        .arg(&batch_path);
    for arg in args {
        command.arg(arg);
    }
    command
        .spawn()
        .with_context(|| format!("Kh?ng mo duoc {}", batch_path.display()))?;

    Ok(())
}

fn qwen_service_port_from_url(base_url: &str) -> Result<u16> {
    base_url
        .rsplit(':')
        .next()
        .ok_or_else(|| anyhow!("Kh?ng tach duoc port tu {}", base_url))?
        .parse::<u16>()
        .with_context(|| format!("Port Qwen Service khong hop le trong {}", base_url))
}

fn ensure_qwen_service_background(base_url: &str) -> Result<bool> {
    let port = qwen_service_port_from_url(base_url)?;
    if is_local_port_open(port) {
        return Ok(false);
    }

    let guard = qwen_restart_guard();
    let mut last_restart = guard.lock().expect("qwen restart guard poisoned");
    if let Some(last) = *last_restart {
        if last.elapsed() < Duration::from_secs(QWEN_RESTART_COOLDOWN_SECS) {
            return Ok(false);
        }
    }

    launch_qwen_service_process(port, true)?;
    *last_restart = Some(Instant::now());
    Ok(true)
}

fn get_qwen_service_health(base_url: &str) -> (bool, bool) {
    let url = format!("{}/health", base_url);
    let response = match ureq::get(&url)
        .config()
        .timeout_connect(Some(Duration::from_millis(300)))
        .timeout_global(Some(Duration::from_millis(800)))
        .timeout_recv_response(Some(Duration::from_millis(800)))
        .build()
        .call()
    {
        Ok(resp) => resp,
        Err(_) => return (false, false),
    };

    let body = match response.into_body().read_to_string() {
        Ok(text) => text,
        Err(_) => return (true, false),
    };
    let json: Value = match serde_json::from_str(&body) {
        Ok(json) => json,
        Err(_) => return (true, false),
    };
    let service_ok = json.get("ok").and_then(Value::as_bool).unwrap_or(false);
    let model_ready = json
        .get("model_ready")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    (service_ok, model_ready)
}

fn get_qwen_service_health_all() -> Vec<(bool, bool)> {
    QWEN_SERVICE_URLS
        .iter()
        .map(|url| get_qwen_service_health(url))
        .collect()
}

fn primary_qwen_service_url() -> Option<String> {
    for url in QWEN_SERVICE_URLS {
        let (service_ready, model_ready) = get_qwen_service_health(url);
        if service_ready && model_ready {
            return Some((*url).to_string());
        }
    }
    for url in QWEN_SERVICE_URLS {
        let (service_ready, _) = get_qwen_service_health(url);
        if service_ready {
            return Some((*url).to_string());
        }
    }
    None
}

fn open_in_browser(url: &str) -> Result<()> {
    Command::new("cmd")
        .args(["/C", "start", "", url])
        .spawn()
        .with_context(|| format!("Kh?ng mo duoc URL {}", url))?;
    Ok(())
}

fn open_path_in_explorer(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("Kh?ng tao duoc thu muc {}", path.display()))?;
    Command::new("explorer")
        .arg(path)
        .spawn()
        .with_context(|| format!("Kh?ng mo duoc thu muc {}", path.display()))?;
    Ok(())
}

fn is_local_port_open(port: u16) -> bool {
    let addr = match ("127.0.0.1", port).to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => return false,
        },
        Err(_) => return false,
    };

    TcpStream::connect_timeout(&addr, Duration::from_millis(40)).is_ok()
}
fn video_font_candidates() -> [(&'static str, &'static str, &'static str); 8] {
    [
        ("Tahoma", "Tahoma", r"C:\Windows\Fonts\tahoma.ttf"),
        ("Segoe UI", "SegoeUI", r"C:\Windows\Fonts\segoeui.ttf"),
        ("Arial", "Arial", r"C:\Windows\Fonts\arial.ttf"),
        ("Verdana", "Verdana", r"C:\Windows\Fonts\verdana.ttf"),
        ("Times New Roman", "TimesNewRoman", r"C:\Windows\Fonts\times.ttf"),
        ("Georgia", "Georgia", r"C:\Windows\Fonts\georgia.ttf"),
        ("Trebuchet MS", "TrebuchetMS", r"C:\Windows\Fonts\trebuc.ttf"),
        ("Consolas", "Consolas", r"C:\Windows\Fonts\consola.ttf"),
    ]
}

fn install_app_fonts(ctx: &egui::Context) {
    static APP_FONTS_INSTALLED: OnceLock<()> = OnceLock::new();
    if APP_FONTS_INSTALLED.get().is_some() {
        return;
    }

    let mut fonts = egui::FontDefinitions::default();
    let mut proportional_fallback: Option<String> = None;

    for (display_name, font_key, path) in video_font_candidates() {
        if let Ok(bytes) = fs::read(path) {
            fonts.font_data.insert(
                font_key.to_string(),
                egui::FontData::from_owned(bytes).into(),
            );
            fonts.families.insert(
                egui::FontFamily::Name(display_name.into()),
                vec![font_key.to_string()],
            );
            if proportional_fallback.is_none() {
                proportional_fallback = Some(font_key.to_string());
            }
        }
    }

    if let Some(fallback) = proportional_fallback {
        if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
            family.insert(0, fallback.clone());
        }
        if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Monospace) {
            family.insert(0, fallback);
        }
    }
    ctx.set_fonts(fonts);
    let _ = APP_FONTS_INSTALLED.set(());
}

fn apply_vietnamese_font(ctx: &egui::Context) {
    install_app_fonts(ctx);
}

fn ensure_video_preview_font(ctx: &egui::Context, _font_name: &str) {
    install_app_fonts(ctx);
}

fn video_preview_font_family(font_name: &str) -> egui::FontFamily {
    egui::FontFamily::Name(font_name.to_string().into())
}

fn main() -> Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 920.0])
            .with_min_inner_size([980.0, 720.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Instant Gemini Live TTS",
        options,
        Box::new(|cc| {
            apply_vietnamese_font(&cc.egui_ctx);
            Ok(Box::new(TtsApp::default()))
        }),
    )
    .map_err(|err| anyhow!("Kh?ng khoi dong duoc UI: {err}"))
}







