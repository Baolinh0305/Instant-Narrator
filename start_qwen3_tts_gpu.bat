@echo off
setlocal
set HF_HUB_DISABLE_SYMLINKS_WARNING=1
set PYTHONIOENCODING=utf-8
set "APP_ROOT=%~dp0"
set "HF_HOME=%APP_ROOT%models-cache\huggingface"
set "HF_HUB_CACHE=%HF_HOME%\hub"
set "HF_ASSETS_CACHE=%HF_HOME%\assets"
set "HF_DATASETS_CACHE=%HF_HOME%\datasets"
set "HUGGINGFACE_HUB_CACHE=%HF_HOME%\hub"
set "HUGGINGFACE_ASSETS_CACHE=%HF_HOME%\assets"
set "PYTORCH_PRETRAINED_BERT_CACHE="
set "PYTORCH_TRANSFORMERS_CACHE="
set "TRANSFORMERS_CACHE="
set "HF_HUB_DISABLE_XET=1"
set "TORCH_HOME=%APP_ROOT%models-cache\torch"
set "PIP_CACHE_DIR=%APP_ROOT%models-cache\pip"
set "TRITON_CACHE_DIR=%APP_ROOT%models-cache\triton"
set "CUDA_CACHE_PATH=%APP_ROOT%models-cache\cuda"
set "TORCHINDUCTOR_CACHE_DIR=%APP_ROOT%models-cache\torchinductor"
set "PYTHONPYCACHEPREFIX=%APP_ROOT%models-cache\pycache"
set "NUMBA_CACHE_DIR=%APP_ROOT%models-cache\numba"
set "GRADIO_TEMP_DIR=%APP_ROOT%models-cache\gradio"
set "XDG_CACHE_HOME=%APP_ROOT%models-cache"
set "TEMP=%APP_ROOT%models-cache\tmp"
set "TMP=%APP_ROOT%models-cache\tmp"
if not exist "%HF_HUB_CACHE%" mkdir "%HF_HUB_CACHE%"
if not exist "%HF_ASSETS_CACHE%" mkdir "%HF_ASSETS_CACHE%"
if not exist "%HF_DATASETS_CACHE%" mkdir "%HF_DATASETS_CACHE%"
if not exist "%TORCH_HOME%" mkdir "%TORCH_HOME%"
if not exist "%PIP_CACHE_DIR%" mkdir "%PIP_CACHE_DIR%"
if not exist "%TRITON_CACHE_DIR%" mkdir "%TRITON_CACHE_DIR%"
if not exist "%CUDA_CACHE_PATH%" mkdir "%CUDA_CACHE_PATH%"
if not exist "%TORCHINDUCTOR_CACHE_DIR%" mkdir "%TORCHINDUCTOR_CACHE_DIR%"
if not exist "%PYTHONPYCACHEPREFIX%" mkdir "%PYTHONPYCACHEPREFIX%"
if not exist "%NUMBA_CACHE_DIR%" mkdir "%NUMBA_CACHE_DIR%"
if not exist "%GRADIO_TEMP_DIR%" mkdir "%GRADIO_TEMP_DIR%"
if not exist "%TEMP%" mkdir "%TEMP%"
set "SOX_DIR=%LOCALAPPDATA%\Microsoft\WinGet\Packages\ChrisBagwell.SoX_Microsoft.Winget.Source_8wekyb3d8bbwe\sox-14.4.2"
set "PATH=%SOX_DIR%;%PATH%"
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*simple_qwen_ui.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" >nul 2>&1
timeout /t 1 /nobreak >nul
call "%~dp0engines\qwen3tts\.venv\Scripts\python.exe" "%~dp0simple_qwen_ui.py" --host 127.0.0.1 --port 7861 --device cuda:0 --dtype float16
if errorlevel 1 (
  echo.
  echo Qwen3-TTS da thoat voi loi. Nhan phim bat ky de dong cua so nay.
  pause >nul
)
