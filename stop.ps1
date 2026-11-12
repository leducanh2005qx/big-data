$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot
docker compose stop
if ($LASTEXITCODE -ne 0) { throw "docker compose stop failed" }
Write-Host "Services stopped; named volumes were preserved."
