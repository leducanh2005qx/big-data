$ErrorActionPreference = "Stop"
& ".\.venv\Scripts\python.exe" -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "Offline tests failed" }
docker compose config --quiet
if ($LASTEXITCODE -ne 0) { throw "Compose validation failed" }
docker info *> $null
if ($LASTEXITCODE -ne 0) { throw "Docker Engine unavailable; integration tests are NOT TESTED" }
docker compose ps
if ($LASTEXITCODE -ne 0) { throw "Cannot inspect Compose services" }
& ".\.venv\Scripts\python.exe" -m producer.open_meteo_collector --once
if ($LASTEXITCODE -ne 0) { throw "Open-Meteo integration failed" }
Write-Host "Pipeline smoke test completed; inspect Spark, InfluxDB and Grafana evidence before marking integration PASS."
