# Laptop 2: Benchmark Client Startup Script
# Spec: i3, 4GB RAM

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  ROCKSDB DISTRIBUTED: CLIENT TIER (Laptop 2)" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "This machine generates load."

$apiIp1 = "192.168.0.211"
$apiIp2 = "192.168.0.130"

Write-Host "Starting Benchmark Client targeting API Proxies ($apiIp1, $apiIp2)..." -ForegroundColor Green
# Start JVM with low memory footprint (Machine only has 4GB)
java -Xms512M -Xmx1G -cp "target\rocksdb-distributed-1.0-SNAPSHOT.jar;target\dependency\*" com.benchmark.distributed.client.BenchmarkClient
