$env:MAVEN_OPTS="-Xms1G -Xmx1G -XX:+UseZGC -XX:+ZGenerational -XX:+AlwaysPreTouch"
Write-Host "Starting gRPC RocksDB Server with optimized JVM..." -ForegroundColor Cyan
mvn exec:java "-Dexec.mainClass=com.benchmark.GrpcRocksDBServer"
