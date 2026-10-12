@echo off
cd /d "%~dp0"
set "ARG=%~1"

if exist agentlink.json goto :run

if defined ARG goto :witharg

echo ==================================================
echo  AgentLink 首次配置 - 二选一：
echo.
echo   [1] 免密快速测试:  双击本脚本之前在命令行执行
echo       启动测试.bat --open
echo.
echo   [2] 自动配对: 先在主机上运行  agentlink token
echo       拿到 cluster_token, 然后执行
echo       启动测试.bat ^<token^>
echo.
echo  （不需要打开或修改 agentlink.json）
echo ==================================================
echo.
pause
exit /b

:witharg
if /i "%ARG%"=="--open" (
    "%~dp0agentlink.exe" init --name agent-LAN-TEST --open --force
    if errorlevel 1 ( echo 初始化失败 & pause & exit /b )
    echo 已进入免密模式.
    goto :run
)
"%~dp0agentlink.exe" join "%ARG%" --name agent-LAN-TEST --force
if errorlevel 1 ( echo 配对失败，请检查 token 是否正确 & pause & exit /b )
echo 配对成功.

:run
"%~dp0agentlink.exe" run
pause
