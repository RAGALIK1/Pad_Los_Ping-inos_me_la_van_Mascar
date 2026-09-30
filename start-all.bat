@echo off
REM Porneste brokerul si interfata web in ferestre separate
cd /d "%~dp0"
chcp 65001 >nul
start "Broker (Python)" cmd /k python broker-python\broker.py --data-dir broker-python\data
timeout /t 1 >nul
start "Interfata web (Node.js)" cmd /k node gateway-node\server.js
timeout /t 2 >nul
start http://localhost:8080
