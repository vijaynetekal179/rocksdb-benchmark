# Laptop 1: API Proxy Server #1 Startup Script
# Spec: Ryzen 5, 8GB RAM

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  ROCKSDB DISTRIBUTED: API PROXY #1 (Laptop 1)" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Find local IP to display to the user
$ip = (Test-Connection -ComputerName (hostname) -Count 1).IPV4Address.IPAddressToString
Write-Host "=> Your API Proxy IP should be: 192.168.0.211" -ForegroundColor Yellow
Write-Host "=> (Detected local IP: $ip)" -ForegroundColor DarkGray
Write-Host "=> Port: 50051" -ForegroundColor Yellow
Write-Host "------------------------------------------"

$dbIp = "192.168.0.118"

Write-Host "Starting API Proxy #1 connected to DB ($dbIp)..." -ForegroundColor Green
# Start JVM with moderate memory for proxying
java -Xms1G -Xmx4G -cp "target\rocksdb-distributed-1.0-SNAPSHOT.jar;target\dependency\*" com.benchmark.distributed.proxy.ApiProxyServer
