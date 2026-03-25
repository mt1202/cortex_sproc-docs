@echo off
setlocal

echo ========================================
echo Launching Streamlit via WSL
echo ========================================

REM Convert Windows path to WSL path
for /f "delims=" %%i in ('wsl wslpath "%~dp0"') do set WSL_PATH=%%i

echo WSL project path: %WSL_PATH%

REM Run inside WSL and keep terminal open
wsl bash -lc "cd '%WSL_PATH%/..' && chmod +x scripts/launch_streamlit.sh && ./scripts/launch_streamlit.sh; echo; echo Press ENTER to exit; read"

echo.
echo If nothing happened, check:
echo - WSL is installed
echo - Python is installed in WSL
echo - Streamlit is installed
pause