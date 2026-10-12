# Install AgentLink as a Windows service.
# Run from an elevated PowerShell ("Run as Administrator").
# Usage: .\install-windows.ps1 [-Binary C:\bin\agentlink.exe] [-Config C:\etc\agentlink.json] [-Name agentlink]

param(
    [string]$Binary = "$PSScriptRoot\..\target\release\agentlink.exe",
    [string]$Config = "C:\ProgramData\agentlink\agentlink.json",
    [string]$Name = "agentlink"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $Binary)) {
    Write-Host "binary not found: $Binary" -ForegroundColor Red
    exit 1
}

# Data directory
$DataDir = "C:\ProgramData\agentlink"
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

# First-run config generation
if (-not (Test-Path $Config)) {
    Push-Location $DataDir
    & $Binary --config $Config init --name "$env:COMPUTERNAME"
    Pop-Location
    Write-Host "generated config: $Config — review it before starting the service" -ForegroundColor Yellow
}

# Register as a service (create if missing, otherwise restart)
$existing = Get-Service -Name $Name -ErrorAction SilentlyContinue
if (-not $existing) {
    New-Service -Name $Name `
        -BinaryPathName "`"$Binary`" --config `"$Config`" run" `
        -DisplayName "AgentLink" `
        -Description "AgentLink LAN message bus for AI agents" `
        -StartupType Automatic | Out-Null
    Write-Host "service created: $Name"
} else {
    Write-Host "service already exists: $Name"
    Restart-Service -Name $Name -ErrorAction SilentlyContinue
}

# Firewall rules (elevated)
$need = $false
try { Get-NetFirewallRule -DisplayName "AgentLink" | Out-Null } catch { $need = $true }
if ($need) {
    New-NetFirewallRule -DisplayName "AgentLink" -Direction Inbound -Protocol TCP -LocalPort 8765 -Action Allow | Out-Null
    New-NetFirewallRule -DisplayName "AgentLink" -Direction Inbound -Protocol UDP -LocalPort 8766 -Action Allow | Out-Null
    Write-Host "firewall rules added (TCP 8765, UDP 8766)"
}

Start-Service -Name $Name
Write-Host "service started: $Name"
Write-Host "check: Get-Service $Name ; curl http://127.0.0.1:8765/v1/health"