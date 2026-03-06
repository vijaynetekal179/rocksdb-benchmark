# Laptop 2: API Proxy Server Startup Script
# Spec: Ryzen 5, 8GB RAM

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  ROCKSDB DISTRIBUTED: API PROXY TIER" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Find local IP to display to the user
$ip = (Test-Connection -ComputerName (hostname) -Count 1).IPV4Address.IPAddressToString
Write-Host "=> Your API Proxy IP is: $ip" -ForegroundColor Yellow
Write-Host "=> Port: 50051" -ForegroundColor Yellow
Write-Host "=> Give this IP to Laptop 1 (Client)." -ForegroundColor Yellow
Write-Host "------------------------------------------"

$dbIp = "192.168.0.130"

Write-Host "Starting API Proxy connected to DB ($dbIp)..." -ForegroundColor Green
# Start JVM with moderate memory for proxying
java -Xms1G -Xmx4G -cp "target\rocksdb-distributed-1.0-SNAPSHOT.jar;target\dependency\*" com.benchmark.distributed.proxy.ApiProxyServer $dbIp
