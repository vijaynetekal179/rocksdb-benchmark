$env:MAVEN_OPTS="-Xms1G -Xmx1G -XX:+UseZGC -XX:+ZGenerational -XX:+AlwaysPreTouch"
Write-Host "Starting gRPC Redis Client with optimized JVM..." -ForegroundColor Green
mvn exec:java "-Dexec.mainClass=com.benchmark.GrpcRedisClient"
