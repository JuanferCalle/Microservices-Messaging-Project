<#
Start-Forwards.ps1

Inicia ventanas de PowerShell separadas con `kubectl port-forward` para los servicios
`monitor`, `admission` y `rabbitmq` en el namespace `hospital`.

Uso:
  .\start-forwards.ps1

Opciones (ejemplo):
  .\start-forwards.ps1 -MonitorPort 8000 -AdmissionPort 8001 -RabbitPort 15672 -Namespace hospital

El script guarda los PIDs de las ventanas creadas en `forward-pids.json`.
#>

param(
    [int]$MonitorPort = 8000,
    [int]$AdmissionPort = 8001,
    [int]$RabbitPort = 15672,
    [string]$Namespace = 'hospital'
)

Set-Location -Path $PSScriptRoot

$pids = @()

Write-Host "Starting port-forward for monitor -> localhost:$MonitorPort (service/monitor:8000)"
$argsMon = "-NoExit","-Command","kubectl port-forward -n $Namespace svc/monitor $MonitorPort:8000"
$procMon = Start-Process -FilePath powershell -ArgumentList $argsMon -PassThru
$pids += $procMon.Id

Write-Host "Starting port-forward for admission -> localhost:$AdmissionPort (service/admission:8000)"
$argsAdm = "-NoExit","-Command","kubectl port-forward -n $Namespace svc/admission $AdmissionPort:8000"
$procAdm = Start-Process -FilePath powershell -ArgumentList $argsAdm -PassThru
$pids += $procAdm.Id

Write-Host "Starting port-forward for rabbitmq -> localhost:$RabbitPort (service/rabbitmq:15672)"
$argsRab = "-NoExit","-Command","kubectl port-forward -n $Namespace svc/rabbitmq $RabbitPort:15672"
$procRab = Start-Process -FilePath powershell -ArgumentList $argsRab -PassThru
$pids += $procRab.Id

$json = @{ pids = $pids; startedAt = (Get-Date).ToString('o') } | ConvertTo-Json
$json | Out-File -FilePath .\forward-pids.json -Encoding UTF8

Write-Host "Saved forward PIDs to forward-pids.json: $($pids -join ',')"
Write-Host "You can open http://localhost:$MonitorPort in your browser to view the monitor UI."
