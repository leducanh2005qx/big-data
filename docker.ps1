param(
    [ValidateSet("help", "start", "stop", "status", "logs", "rebuild")]
    [string] $Action = "help"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

switch ($Action) {
    "start" {
        & "$ProjectRoot\start.ps1"
        break
    }
    "stop" {
        & "$ProjectRoot\stop.ps1"
        break
    }
    "status" {
        & "$ProjectRoot\status.ps1"
        break
    }
    "logs" {
        docker compose logs --tail 100 -f
        break
    }
    "rebuild" {
        docker compose up -d --build --force-recreate
        docker compose ps
        break
    }
    default {
        Write-Host "AirQuality Big Data - Docker commands"
        Write-Host ""
        Write-Host "  .\docker.ps1 start    Khoi dong toan bo stack va giu named volumes"
        Write-Host "  .\docker.ps1 status   Xem trang thai Docker, InfluxDB va Grafana"
        Write-Host "  .\docker.ps1 logs     Theo doi log cua tat ca service"
        Write-Host "  .\docker.ps1 rebuild  Build lai image va recreate service"
        Write-Host "  .\docker.ps1 stop     Dung service, khong xoa du lieu"
        Write-Host ""
        Write-Host "Khong dung 'docker compose down -v' neu muon giu du lieu demo."
    }
}
