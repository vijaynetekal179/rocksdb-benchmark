# Laptop 3: Database Database Server Startup Script
# Spec: i5-12500H, 16GB RAM, NVMe SSD

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  ROCKSDB DISTRIBUTED: DB SERVER TIER" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Find local IP to display to the user
$ip = (Test-Connection -ComputerName (hostname) -Count 1).IPV4Address.IPAddressToString
Write-Host "=> Your DB Server IP is: $ip" -ForegroundColor Yellow
Write-Host "=> Port: 50052" -ForegroundColor Yellow
Write-Host "=> Give this IP to Laptop 2 (API Proxy)." -ForegroundColor Yellow
Write-Host "------------------------------------------"

# Ensure the DB directory exists early on
$dbPath = "C:\distributed-rocksdb-data"
if (-Not (Test-Path $dbPath)) {
    New-Item -ItemType Directory -Force -Path $dbPath | Out-Null
    Write-Host "Created new RocksDB data directory at $dbPath"
}

# Run the JVM 
Write-Host "Starting Database Server..." -ForegroundColor Green
# We give it plenty of memory to use for RocksDB block cache
java -Xms4G -Xmx8G -cp "target\rocksdb-distributed-1.0-SNAPSHOT.jar;target\dependency\*" com.benchmark.distributed.server.RocksDbServer
