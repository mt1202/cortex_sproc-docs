@echo off
setlocal
wsl bash -lc "cd '$(wslpath "%~dp0")/..' && python -m runner.cli %*"
endlocal
