@echo off
cd /d "%~dp0"
set "ARG=%~1"
title AgentLink LAN Node

rem 已有一个实例在 8765 端口跑着的话，就不重复启动
curl.exe -s -m 1 http://127.0.0.1:8765/v1/health >nul 2>&1
if not errorlevel 1 (
    echo.
    echo AgentLink 已经在运行 127.0.0.1:8765, 无需重复启动。
    echo 如要停止，请关掉之前那个命令行窗口。
    echo.
    pause
    exit /b
)

if defined ARG goto :witharg

rem 没有参数：确保有配置，自动免密初始化并启动
if not exist agentlink.json (
    "%~dp0agentlink.exe" init --name agent-LAN-TEST --open --force
    if errorlevel 1 (
        echo [错误] 初始化失败。请确认本目录下有 agentlink.exe。
        pause
        exit /b
    )
    echo 已自动创建配置（免密互信模式 --open）。
    echo 局域网内所有机器双击本脚本即可互通。
)
goto :run

:witharg
if /i "%ARG%"=="--strict" (
    "%~dp0agentlink.exe" init --name agent-LAN-TEST --force
    echo 已创建严格模式配置（见上方输出中的 cluster_token，发给其他机器配对）。
    goto :run
)
"%~dp0agentlink.exe" join "%ARG%" --name agent-LAN-TEST --force
if errorlevel 1 (
    echo [错误] 配对失败，请检查 token 是否与主机一致。
    pause
    exit /b
)
echo 配对成功（strict 模式）.

:run
echo 启动 AgentLink... （关闭本窗口即停止）
"%~dp0agentlink.exe" run
echo.
echo [已退出] 常见原因：
echo   - 端口 8765 被占用（改 agentlink.json 的 listen_port）
echo   - Windows 防火墙未放行 8765/8766
echo   - 运行异常（可看上方日志）
echo.
pause
