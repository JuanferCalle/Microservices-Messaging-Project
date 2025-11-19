<#
Stop-Forwards.ps1

Lee el archivo `forward-pids.json` creado por `start-forwards.ps1` y termina
los procesos (ventanas de PowerShell) que contienen los `kubectl port-forward`.

Uso:
  .\stop-forwards.ps1

#>
Set-Location -Path $PSScriptRoot

$pidFile = Join-Path $PSScriptRoot 'forward-pids.json'
if (-not (Test-Path $pidFile)) {
    Write-Warning "No se encontró forward-pids.json. No hay forwards guardados."
    exit 0
}

try {
    $data = Get-Content -Raw -Path $pidFile | ConvertFrom-Json
    foreach ($id in $data.pids) {
        try {
            Write-Host "Stopping process PID $id ..."
            Stop-Process -Id $id -Force -ErrorAction Stop
        } catch {
            Write-Warning "No se pudo detener PID $id o ya no existe: $_"
        }
    }
    Remove-Item -Path $pidFile -ErrorAction SilentlyContinue
    Write-Host "Stopped forwards and removed forward-pids.json"
} catch {
    Write-Error "Error leyendo forward-pids.json: $_"
}
