# Laptop 1: Benchmark Client Server Startup Script
# Spec: i3, 4GB RAM

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  ROCKSDB DISTRIBUTED: CLIENT TIER" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "This machine generates load."

$apiIp = "192.168.0.211"

Write-Host "Starting Benchmark Client targeting API Proxy ($apiIp)..." -ForegroundColor Green
# Start JVM with low memory footprint (Machine only has 4GB)
java -Xms512M -Xmx1G -cp "target\rocksdb-distributed-1.0-SNAPSHOT.jar;target\dependency\*" com.benchmark.distributed.client.BenchmarkClient $apiIp
