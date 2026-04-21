$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$body = @{
    message = "I need an estimate for an ADU project"
    user_id = "test_user"
    channel = "phone"
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://127.0.0.1:8080/test-webhook" `
  -Method POST `
  -Body $body `
  -ContentType "application/json"

$confirmationContext = @{
    session_id = $response.result.body.session_id
    user_id = $response.result.payload.user_id
    user_role = $response.result.payload.user_role
    contact_id = $response.result.payload.contact_id
    channel = $response.result.payload.channel
    confirmation_required = $response.result.body.confirmation_required
    confirmation_prompt = $response.result.body.confirmation_prompt
} | ConvertTo-Json

Set-Content -LiteralPath ".\data\test_confirmation_context.json" -Value $confirmationContext -Encoding UTF8
$response
