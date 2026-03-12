@echo off
setlocal
set HF_HUB_DISABLE_SYMLINKS_WARNING=1
set PYTHONIOENCODING=utf-8
set "APP_ROOT=%~dp0"
set "HF_HOME=%APP_ROOT%models-cache\huggingface"
set "HF_HUB_CACHE=%HF_HOME%\hub"
set "HUGGINGFACE_HUB_CACHE=%HF_HOME%\hub"
set "TRANSFORMERS_CACHE=%HF_HOME%\transformers"
set "TORCH_HOME=%APP_ROOT%models-cache\torch"
set "XDG_CACHE_HOME=%APP_ROOT%models-cache"
set "TEMP=%APP_ROOT%models-cache\tmp"
set "TMP=%APP_ROOT%models-cache\tmp"
set "FFMPEG_SHARED_BIN=C:\Users\ngbal\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg.Shared_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.0.1-full_build-shared\bin"
if not exist "%HF_HUB_CACHE%" mkdir "%HF_HUB_CACHE%"
if not exist "%TRANSFORMERS_CACHE%" mkdir "%TRANSFORMERS_CACHE%"
if not exist "%TORCH_HOME%" mkdir "%TORCH_HOME%"
if not exist "%TEMP%" mkdir "%TEMP%"
if exist "%FFMPEG_SHARED_BIN%" set "PATH=%FFMPEG_SHARED_BIN%;%PATH%"
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*simple_f5_ui.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" >nul 2>&1
timeout /t 1 /nobreak >nul
call "%~dp0engines\f5tts\.venv\Scripts\python.exe" "%~dp0simple_f5_ui.py" --host 127.0.0.1 --port 7860
