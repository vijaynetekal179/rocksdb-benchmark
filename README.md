# 🚀 gRPC Database Benchmark: RocksDB vs Redis

A high-performance benchmarking framework that compares **RocksDB** (embedded LSM-tree database) against **Redis** (in-memory store) under sustained, concurrent, realistic mixed-workload conditions using **gRPC bi-directional streaming** over TLS.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Benchmark Results](#benchmark-results)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup & Running](#setup--running)
- [How It Works](#how-it-works)
- [Threading Model](#threading-model)
- [Security](#security)

---

## 📖 Project Overview

This project implements a **client-server gRPC benchmark** that stress-tests two fundamentally different database backends with 1,000,000 pre-seeded user records under a **mixed workload** of:
- **50% READs**
- **30% UPDATEs**
- **20% DELETEs**

Three separate server implementations are benchmarked:

| Implementation | Class | Transport | Atomicity |
|---|---|---|---|
| RocksDB | `GrpcRocksDBServer` | Embedded (in-JVM) | Optimistic Transactions + WAL |
| Redis Pipelines | `GrpcRedisServer` | TCP Network Hop | None (best-effort batching) |
| Redis MULTI/EXEC | `GrpcRedisTxServer` | TCP Network Hop | True Atomic Transactions |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      CLIENT MACHINE                         │
│                                                             │
│  GrpcRocksDBClient / GrpcRedisClient / GrpcRedisTxClient   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  96 Worker Threads (12 CPUs × 8)                     │  │
│  │  Each thread: gRPC Bi-Directional Stream              │  │
│  │  Sends: OperationBatch (200 ops/batch) via Protobuf   │  │
│  │  Receives: BatchResult (success/fail)                 │  │
│  └──────────────────────────────────────────────────────┘  │
│                         │ TLS / HTTP/2                      │
└─────────────────────────┼───────────────────────────────────┘
                          │
┌─────────────────────────┼───────────────────────────────────┐
│                      SERVER MACHINE                         │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Netty gRPC Server (Port 9090 / 9091 / 9092)        │    │
│  │  JwtServerInterceptor → Auth Check (JWT Bearer)     │    │
│  │  24 Executor Threads (12 CPUs × 2)                  │    │
│  └──────────────────┬─────────────────────────────────┘    │
│                     │                                        │
│         ┌───────────┴──────────────┐                        │
│         ▼                          ▼                        │
│  ┌─────────────┐          ┌────────────────┐               │
│  │  RocksDB    │          │  Redis Server  │               │
│  │  (On Disk)  │          │  (localhost:   │               │
│  │  Port 9090  │          │  6379)         │               │
│  │  WriteBatch │          │  Pipeline /    │               │
│  │  + WAL      │          │  MULTI/EXEC    │               │
│  └─────────────┘          └────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Benchmark Results

> Run on **AMD Ryzen 5** (6 Cores / 12 Logical Processors), Windows 10, local loopback

| Metric | RocksDB (1 Hr) | Redis Pipelines (1 Hr) | Redis MULTI/EXEC (1 Min) |
|---|---|---|---|
| **TPS** | **766,550 ops/sec** | 474,097 ops/sec | 212,771 ops/sec |
| **Total Ops** | 3,263,069,400 | 1,735,697,800 | 30,570,200 |
| **Successful Ops** | 2,794,243,400 | 1,735,697,800 | 30,570,200 |
| **Failed Ops** | 468,826,000 | 0 | 0 |
| **Avg Batch RTT** | 34,417.41 ms | 65,144.17 ms | 76,627.51 ms |
| **Avg Op RTT** | **172.09 ms** | 325.72 ms | 383.14 ms |
| **Batches Sent** | 16,315,347 | 8,678,489 | 152,851 |
| **Batch Size** | 200 | 200 | 200 |

### Key Takeaways
- **RocksDB achieved 1.62× higher TPS** than Redis pipelines because it is embedded directly inside the JVM — zero network overhead between the application and the database.
- **Redis Pipelines had 0 failures** vs RocksDB's 468M failed ops — Redis never hit I/O backpressure since everything lives in RAM.
- **Redis MULTI/EXEC (~212K TPS) is ~55% slower** than Redis Pipelines (~474K TPS) because true atomic transactions force Redis to block its single event-loop thread for every batch of 200 commands.

---

## 🛠️ Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 17 | Core language |
| gRPC (Netty Shaded) | 1.59.0 | High-performance RPC transport |
| Protocol Buffers | 3.24.4 | Binary serialization of messages |
| RocksDB (rocksdbjni) | 8.10.0 | Embedded key-value database |
| Jedis | 5.1.0 | Java client for Redis |
| JJWT | 0.12.5 | JWT generation and validation |
| Maven | 3.x | Build system |
| ZGC (JVM Flag) | Generational | Low-latency garbage collection |

---

## 📁 Project Structure

```
rocksdb-benchmark/
│
├── src/
│   └── main/
│       ├── java/com/benchmark/
│       │   ├── GrpcRocksDBServer.java   # RocksDB server (port 9090)
│       │   ├── GrpcRocksDBClient.java   # RocksDB benchmark client
│       │   ├── GrpcRedisServer.java     # Redis Pipeline server (port 9091)
│       │   ├── GrpcRedisClient.java     # Redis Pipeline benchmark client
│       │   ├── GrpcRedisTxServer.java   # Redis MULTI/EXEC server (port 9092)
│       │   ├── GrpcRedisTxClient.java   # Redis Transaction benchmark client
│       │   ├── JwtUtil.java             # JWT token generation (HS256)
│       │   └── JwtServerInterceptor.java# gRPC server-side JWT auth interceptor
│       └── proto/
│           └── benchmark.proto          # Protobuf schema for all messages
│
├── run-rocks-server.ps1   # Launch script for RocksDB server
├── run-rocks-client.ps1   # Launch script for RocksDB client
├── run-redis-server.ps1   # Launch script for Redis Pipeline server
├── run-redis-client.ps1   # Launch script for Redis Pipeline client
├── benchmark.crt          # TLS public certificate (shared with client)
├── benchmark.p12          # TLS keystore (server only, NOT committed)
└── pom.xml                # Maven build definition
```

---

## ✅ Prerequisites

1. **Java 17+** — [Download](https://adoptium.net/)
2. **Apache Maven 3.8+** — [Download](https://maven.apache.org/)
3. **Redis Server** running locally on `localhost:6379`
   - Windows: [Redis for Windows](https://github.com/microsoftarchive/redis/releases) or via WSL (`sudo apt install redis-server`)
   - Start it: `redis-server`
4. **TLS Certificates** — `benchmark.p12` (server keystore) and `benchmark.crt` (public cert) must exist in the project root.

---

## ▶️ Setup & Running

### 1. Build the project
```powershell
mvn compile
```

### 2. Run the RocksDB Benchmark (Port 9090)
```powershell
# Terminal 1 - Server
$env:MAVEN_OPTS="-Xms1G -Xmx1G -XX:+UseZGC -XX:+ZGenerational -XX:+AlwaysPreTouch"
mvn compile exec:java "-Dexec.mainClass=com.benchmark.GrpcRocksDBServer"

# Terminal 2 - Client
mvn compile exec:java "-Dexec.mainClass=com.benchmark.GrpcRocksDBClient"
```

### 3. Run the Redis Pipeline Benchmark (Port 9091)
```powershell
# Terminal 1 - Server (requires Redis running on port 6379)
mvn compile exec:java "-Dexec.mainClass=com.benchmark.GrpcRedisServer"

# Terminal 2 - Client
mvn compile exec:java "-Dexec.mainClass=com.benchmark.GrpcRedisClient"
```

### 4. Run the Redis MULTI/EXEC Benchmark (Port 9092)
```powershell
# Terminal 1 - Server (requires Redis running on port 6379)
mvn compile exec:java "-Dexec.mainClass=com.benchmark.GrpcRedisTxServer"

# Terminal 2 - Client
mvn compile exec:java "-Dexec.mainClass=com.benchmark.GrpcRedisTxClient"
```

> ⚠️ **Important:** Always use `mvn compile exec:java` as a chained command — never `mvn exec:java` alone — to ensure freshly compiled class files are used.

---

## ⚙️ How It Works

### Protobuf Schema (`benchmark.proto`)

```protobuf
service BenchmarkService {
  rpc Authenticate(AuthRequest) returns (AuthResponse);
  rpc StreamOperations(stream OperationBatch) returns (stream BatchResult);
}
```

- **`Authenticate`**: A simple unary RPC. The client sends a password, the server returns a JWT token.
- **`StreamOperations`**: A **bi-directional streaming** RPC. The client continuously streams `OperationBatch` messages (200 ops each), and the server replies with `BatchResult` as each batch is processed.

### Batch Flow
```
Client Thread → Build 200 random ops → gRPC stream → Server → DB write/read → BatchResult ack
```

### Flow Control
The client uses gRPC's `ClientCallStreamObserver.isReady()` to implement backpressure — it pauses briefly if the TCP send buffer is full, preventing out-of-memory crashes under extreme load.

---

## 🧵 Threading Model

> Based on AMD Ryzen 5 (12 Logical Processors)

| Component | Count | Formula | Role |
|---|---|---|---|
| **Client Worker Threads** | 96 | `CPUs × 8` | Simulate 96 concurrent users, each with their own gRPC stream |
| **Server Executor Threads** | 24 | `CPUs × 2` | Process incoming gRPC calls and dispatch to the DB |
| **Netty Event Loop Threads** | 2–4 | Managed by Netty | Low-level NIO — reads raw bytes off TCP socket |
| **RocksDB Background Threads** | 6 | `setMaxBackgroundJobs(6)` | Compaction and MemTable flushing (disk I/O) |
| **Redis Event Loop** | 1 | Built into Redis | Single-threaded command execution loop |

---

## 🔐 Security

- **TLS Encryption**: All gRPC traffic is encrypted using TLS (`benchmark.p12` keystore, `benchmark.crt` public cert). Built with Netty's SslContext.
- **JWT Authentication**: Every client must first call `Authenticate()` with the correct password. The server returns a signed JWT (HS256). All subsequent `StreamOperations` calls must include `Authorization: Bearer <token>` in gRPC metadata, validated by `JwtServerInterceptor`.
- **Connection Pooling**: The Jedis pool is configured with health checks (`testOnBorrow`, `testWhileIdle`) and a 3-minute socket timeout to handle long-running atomic transactions gracefully.
