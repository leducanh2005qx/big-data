param(
    [switch] $Continuous
)
$ErrorActionPreference = "Stop"
if (-not (Test-Path ".env")) { throw ".env is missing; copy .env.example first" }
if (-not (Test-Path ".venv\Scripts\python.exe")) { throw ".venv is missing" }
$arguments = @("-m", "producer.open_meteo_collector")
if ($Continuous) { $arguments += "--continuous" } else { $arguments += "--once" }
& ".\.venv\Scripts\python.exe" @arguments
if ($LASTEXITCODE -ne 0) { throw "Open-Meteo collector failed with exit code $LASTEXITCODE" }
