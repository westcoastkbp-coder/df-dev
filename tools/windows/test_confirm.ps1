$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$contextPath = ".\data\test_confirmation_context.json"
if (-not (Test-Path $contextPath)) {
    throw "Confirmation context not found. Run .\runtime\test_webhook.ps1 first."
}

$context = Get-Content -LiteralPath $contextPath -Raw | ConvertFrom-Json
if (-not $context.session_id) {
    throw "Confirmation context is missing session_id."
}

$body = @{
    message = "yes"
    channel = $context.channel
    contact_id = $context.contact_id
    session_id = $context.session_id
    user_id = $context.user_id
    user_role = $context.user_role
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://127.0.0.1:8080/input" `
  -Method POST `
  -Body $body `
  -ContentType "application/json"
