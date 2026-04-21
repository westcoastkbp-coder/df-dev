$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
.\venv\Scripts\Activate.ps1
python -m uvicorn app.server:app --host 0.0.0.0 --port 8080
