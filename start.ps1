$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

function Fail([string] $Message) {
    Write-Error $Message
    exit 1
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Fail "Docker CLI was not found. Install Docker Desktop and reopen PowerShell."
}

$dockerInfo = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Fail "Docker Engine is unavailable. Start Docker Desktop and verify 'docker info'."
}

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example. Review credentials before sharing the host."
}

$required = @("INFLUXDB_DATABASE", "INFLUXDB_ADMIN_USER", "INFLUXDB_ADMIN_PASSWORD", "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD")
$envText = Get-Content ".env" -Raw
foreach ($name in $required) {
    if ($envText -notmatch "(?m)^$name=\S+") { Fail "Missing required .env value: $name" }
}

Write-Host "Validating Compose configuration..."
docker compose config *> $null
if ($LASTEXITCODE -ne 0) { Fail "docker compose config failed." }

Write-Host "Starting services with persistent named volumes..."
$started = $false
for ($attempt = 1; $attempt -le 2; $attempt++) {
    docker compose up -d --build
    if ($LASTEXITCODE -eq 0) {
        $started = $true
        break
    }

    if ($attempt -eq 1) {
        Write-Warning "Compose startup failed, usually because Kafka is still recovering. Restarting ZooKeeper and retrying once..."
        docker compose restart zookeeper
        if ($LASTEXITCODE -ne 0) { Fail "ZooKeeper recovery failed. Inspect: docker compose logs zookeeper" }
        Start-Sleep -Seconds 25
    }
}
if (-not $started) { Fail "docker compose up failed after retry. Inspect: docker compose logs" }

$deadline = (Get-Date).AddMinutes(3)
do {
    $status = docker compose ps --format json | ConvertFrom-Json
    $unhealthy = @($status | Where-Object { $_.Health -eq "unhealthy" })
    $starting = @($status | Where-Object { $_.Health -eq "starting" })
    if ($unhealthy.Count -gt 0) { Fail "A service is unhealthy: $($unhealthy.Name -join ', ')" }
    if ($starting.Count -eq 0) { break }
    Start-Sleep -Seconds 5
} while ((Get-Date) -lt $deadline)

if ((Get-Date) -ge $deadline) { Fail "Timed out waiting for service health. Inspect: docker compose ps and docker compose logs" }

Write-Host "Initializing Kafka topics and InfluxDB retention policies..."
if (Test-Path ".venv\Scripts\python.exe") {
    .\.venv\Scripts\python.exe -m scripts.influxdb.setup_retention
} else {
    Write-Warning "Python virtual environment not found; run the retention setup manually."
}

docker compose ps
Write-Host "Grafana: http://localhost:3000"
Write-Host "InfluxDB: http://localhost:8086"
Write-Host "Open-Meteo collector: running in Docker with restart policy unless-stopped"
