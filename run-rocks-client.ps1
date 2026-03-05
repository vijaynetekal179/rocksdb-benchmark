$env:MAVEN_OPTS="-Xms1G -Xmx1G -XX:+UseZGC -XX:+ZGenerational -XX:+AlwaysPreTouch"
Write-Host "Starting gRPC RocksDB Client with optimized JVM..." -ForegroundColor Cyan
mvn exec:java "-Dexec.mainClass=com.benchmark.GrpcRocksDBClient"
