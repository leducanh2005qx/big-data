$ErrorActionPreference = "Continue"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot
Write-Host "=== Compose services ==="
docker compose ps
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker Engine is unavailable. Start Docker Desktop and retry."
    exit 1
}
Write-Host "=== InfluxDB health ==="
try { Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8086/ping | Select-Object StatusCode } catch { Write-Warning $_ }
Write-Host "=== Grafana health ==="
try { Invoke-RestMethod http://127.0.0.1:3000/api/health | ConvertTo-Json } catch { Write-Warning $_ }
