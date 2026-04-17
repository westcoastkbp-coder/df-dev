$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
curl.exe http://localhost:8080/health
