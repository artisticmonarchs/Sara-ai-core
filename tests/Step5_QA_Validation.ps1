# ------------------------------------------
# Step5_QA_Validation.ps1 — Sara AI Bot
# ------------------------------------------

$BaseUrl = "https://sara-ai-core-app.onrender.com"

Function Submit-Task {
    param (
        [string]$Endpoint,
        [string]$Payload
    )

    $response = Invoke-WebRequest -Uri "$BaseUrl/$Endpoint" `
        -Method POST `
        -Headers @{ "Content-Type" = "application/json" } `
        -Body $Payload

    $json = $response.Content | ConvertFrom-Json
    return $json.task_id
}

Function Get-TaskResult {
    param (
        [string]$TaskId
    )

    do {
        Start-Sleep -Seconds 3
        $response = Invoke-WebRequest -Uri "$BaseUrl/task-result/$TaskId" -Method GET
        $json = $response.Content | ConvertFrom-Json
    } while ($json.status -eq "pending")

    return $json
}

# --------------------------
# Submit Inference Task
# --------------------------
$inferencePayload = '{"input":"step5_test_inference"}'
$inferenceTaskId = Submit-Task -Endpoint "inference" -Payload $inferencePayload
Write-Host "✅ Inference submitted → $inferenceTaskId"

$inferenceResult = Get-TaskResult -TaskId $inferenceTaskId
Write-Host "Inference Result:"
$inferenceResult | ConvertTo-Json -Depth 5

# --------------------------
# Submit TTS Task
# --------------------------
$ttsPayload = '{"text":"Step 5 TTS test"}'
$ttsTaskId = Submit-Task -Endpoint "tts" -Payload $ttsPayload
Write-Host "✅ TTS submitted → $ttsTaskId"

$ttsResult = Get-TaskResult -TaskId $ttsTaskId
Write-Host "TTS Result:"
$ttsResult | ConvertTo-Json -Depth 5

Write-Host "`n✅ Step 5 QA validation complete. Review trace_ids above for verification."
