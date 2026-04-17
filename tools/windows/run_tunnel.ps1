$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
.\tools\cloudflared.exe tunnel --url http://localhost:8080
