param(
    [string]$PortForwardUrl = "http://localhost:8001/simulate_arrival"
)

# Send the payload in payloads/juan.json to the local admission service (requires port-forwarding)
$payloadPath = Join-Path $PSScriptRoot "payloads\juan.json"
if (-not (Test-Path $payloadPath)) {
    Write-Error "Payload file not found: $payloadPath"
    exit 1
}

$payload = Get-Content -Raw -Path $payloadPath

Write-Host "Posting payload to $PortForwardUrl ..."
try {
    $resp = Invoke-RestMethod -Uri $PortForwardUrl -Method Post -Headers @{"Content-Type"="application/json"} -Body $payload -ErrorAction Stop
    Write-Host "Response:`n" ($resp | ConvertTo-Json -Depth 4)
} catch {
    Write-Host "Request failed:`n$_"
    exit 2
}

Write-Host "Done. Check monitor logs or UI to confirm receipt."
