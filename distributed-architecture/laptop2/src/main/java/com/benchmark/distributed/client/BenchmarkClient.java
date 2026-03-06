package com.benchmark.distributed.client;

import com.benchmark.distributed.grpc.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Laptop 2 (192.168.0.178): Benchmark Client
 *
 * Splits 96 threads evenly across TWO API Proxy Servers:
 * - 48 threads → Laptop 1 (API Server #1): 192.168.0.211:50051
 * - 48 threads → Laptop 3 (API Server #2): 192.168.0.130:50051
 *
 * Both servers forward to RocksDB Server on Laptop 4 (192.168.0.118:50052).
 * Each connection uses TLS + JWT authentication independently.
 */
public class BenchmarkClient {

    // Laptop 1: API Proxy Server #1
    private static final String API_SERVER_1_HOST = "192.168.0.211";
    // Laptop 3: API Proxy Server #2
    private static final String API_SERVER_2_HOST = "192.168.0.130";
    private static final int API_PROXY_PORT = 50051;

    private static final int TOTAL_USERS = 1_000_000;
    private static final int BATCH_SIZE = 200;
    private static final int TOTAL_THREADS = 96;
    private static final int THREADS_PER_SERVER = TOTAL_THREADS / 2; // 48 threads each

    private static final Metadata.Key<String> AUTH_HEADER = Metadata.Key.of("authorization",
            Metadata.ASCII_STRING_MARSHALLER);

    public static void main(String[] args) throws Exception {
        System.out.println("==================================================");
        System.out.println("  Laptop 2 — Benchmark Client (192.168.0.178)    ");
        System.out.println("  Target API Server #1: " + API_SERVER_1_HOST + ":" + API_PROXY_PORT);
        System.out.println("  Target API Server #2: " + API_SERVER_2_HOST + ":" + API_PROXY_PORT);
        System.out.println("  Threads per server: " + THREADS_PER_SERVER);
        System.out.println("==================================================");

        // Load TLS certificate (shared cert used by both API proxy servers)
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate serverCert;
        try (FileInputStream fis = new FileInputStream("benchmark.crt")) {
            serverCert = cf.generateCertificate(fis);
        }

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("benchmark-server", serverCert);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        SslContext sslContext = GrpcSslContexts.configure(SslContextBuilder.forClient().trustManager(tmf)).build();

        // --- Connect to API Server #1 (Laptop 1: 192.168.0.211) ---
        ManagedChannel channel1 = NettyChannelBuilder.forAddress(API_SERVER_1_HOST, API_PROXY_PORT)
                .sslContext(sslContext)
                .overrideAuthority("localhost")
                .build();

        // --- Connect to API Server #2 (Laptop 3: 192.168.0.130) ---
        ManagedChannel channel2 = NettyChannelBuilder.forAddress(API_SERVER_2_HOST, API_PROXY_PORT)
                .sslContext(sslContext)
                .overrideAuthority("localhost")
                .build();

        // --- Authenticate with API Server #1 ---
        ApiServiceGrpc.ApiServiceBlockingStub blockingStub1 = ApiServiceGrpc.newBlockingStub(channel1);
        AuthResponse authResponse1 = blockingStub1.authenticate(
                AuthRequest.newBuilder().setPassword("SuperSecretPassword123!").build());

        if (!authResponse1.getSuccess()) {
            System.err.println("Failed to authenticate to API Server #1 (Laptop 1). Aborting.");
            channel1.shutdown();
            channel2.shutdown();
            return;
        }
        System.out.println("Authenticated with API Server #1 (Laptop 1). JWT received.");

        // --- Authenticate with API Server #2 ---
        ApiServiceGrpc.ApiServiceBlockingStub blockingStub2 = ApiServiceGrpc.newBlockingStub(channel2);
        AuthResponse authResponse2 = blockingStub2.authenticate(
                AuthRequest.newBuilder().setPassword("SuperSecretPassword123!").build());

        if (!authResponse2.getSuccess()) {
            System.err.println("Failed to authenticate to API Server #2 (Laptop 3). Aborting.");
            channel1.shutdown();
            channel2.shutdown();
            return;
        }
        System.out.println("Authenticated with API Server #2 (Laptop 3). JWT received.");

        // Attach JWT tokens to all subsequent calls
        Metadata authMeta1 = new Metadata();
        authMeta1.put(AUTH_HEADER, "Bearer " + authResponse1.getToken());
        ApiServiceGrpc.ApiServiceStub asyncStub1 = ApiServiceGrpc.newStub(channel1)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(authMeta1));

        Metadata authMeta2 = new Metadata();
        authMeta2.put(AUTH_HEADER, "Bearer " + authResponse2.getToken());
        ApiServiceGrpc.ApiServiceStub asyncStub2 = ApiServiceGrpc.newStub(channel2)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(authMeta2));

        // --- PHASE 1: Serial Latency Tests (one per server) ---
        System.out.println("\n--- Serial Latency Test: API Server #1 (Laptop 1) ---");
        double[] latencyStats1 = runSerialLatencyTest(asyncStub1, "Server #1");

        System.out.println("\n--- Serial Latency Test: API Server #2 (Laptop 3) ---");
        double[] latencyStats2 = runSerialLatencyTest(asyncStub2, "Server #2");

        // --- PHASE 2: Throughput Benchmark (96 threads split 48/48) ---
        CountDownLatch latch = new CountDownLatch(TOTAL_THREADS);
        AtomicLong totalBatches = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);
        AtomicLong failedOps = new AtomicLong(0);
        AtomicBoolean isRunning = new AtomicBoolean(true);

        System.out.println("\n🔥 Launching " + TOTAL_THREADS + " threads ("
                + THREADS_PER_SERVER + " → Laptop 1, "
                + THREADS_PER_SERVER + " → Laptop 3)...");

        long globalStartTime = System.currentTimeMillis();

        // 48 threads → API Server #1
        for (int t = 0; t < THREADS_PER_SERVER; t++) {
            launchWorkerThread(asyncStub1, isRunning, totalBatches, successfulOps, failedOps, latch);
        }

        // 48 threads → API Server #2
        for (int t = 0; t < THREADS_PER_SERVER; t++) {
            launchWorkerThread(asyncStub2, isRunning, totalBatches, successfulOps, failedOps, latch);
        }

        // Run for 60 seconds
        Thread.sleep(60_000);
        System.out.println("\n⏳ Benchmark duration reached (60s). Waiting for in-flight requests to complete...");
        isRunning.set(false);

        // Wait up to 3 minutes for all streams to finish their current batches
        // gracefully
        boolean completedGracefully = latch.await(3, TimeUnit.MINUTES);
        if (!completedGracefully) {
            System.err.println("⚠️ Warning: Not all streams completed within the 3-minute grace period!");
        } else {
            System.out.println("✅ All in-flight requests completed successfully.");
        }

        long globalEndTime = System.currentTimeMillis();

        channel1.shutdown().awaitTermination(1, TimeUnit.MINUTES);
        channel2.shutdown().awaitTermination(1, TimeUnit.MINUTES);

        long totalOps = successfulOps.get() + failedOps.get();
        printResults(globalStartTime, globalEndTime, totalOps, successfulOps.get(), failedOps.get(),
                totalBatches.get(), latencyStats1, latencyStats2);
    }

    private static void launchWorkerThread(
            ApiServiceGrpc.ApiServiceStub asyncStub,
            AtomicBoolean isRunning,
            AtomicLong totalBatches,
            AtomicLong successfulOps,
            AtomicLong failedOps,
            CountDownLatch latch) {
        new Thread(() -> {
            try {
                CountDownLatch streamLatch = new CountDownLatch(1);
                StreamObserver<OperationBatch> requestObserver = asyncStub.streamOperations(
                        new StreamObserver<BatchResult>() {
                            @Override
                            public void onNext(BatchResult value) {
                                totalBatches.incrementAndGet();
                                if (value.getSuccess()) {
                                    successfulOps.addAndGet(BATCH_SIZE);
                                } else {
                                    failedOps.addAndGet(BATCH_SIZE);
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                failedOps.addAndGet(BATCH_SIZE);
                                streamLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                streamLatch.countDown();
                            }
                        });

                Random rand = new Random();
                while (isRunning.get()) {
                    OperationBatch.Builder batchBuilder = OperationBatch.newBuilder();
                    for (int b = 0; b < BATCH_SIZE; b++) {
                        int userId = rand.nextInt(TOTAL_USERS);
                        int chance = rand.nextInt(100);
                        Operation.OpType type = (chance < 50) ? Operation.OpType.READ
                                : (chance < 80) ? Operation.OpType.UPDATE : Operation.OpType.DELETE;
                        batchBuilder.addOperations(
                                Operation.newBuilder().setUserId(userId).setType(type).build());
                    }

                    requestObserver.onNext(batchBuilder.build());

                    // Smart gRPC flow control
                    ClientCallStreamObserver<OperationBatch> callObserver = (ClientCallStreamObserver<OperationBatch>) requestObserver;
                    while (isRunning.get() && !callObserver.isReady()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }

                requestObserver.onCompleted();
                streamLatch.await(3, TimeUnit.MINUTES);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }).start();
    }

    private static void printResults(long startMs, long endMs, long totalOps, long successfulOps, long failedOps,
            long totalBatches, double[] lat1, double[] lat2) {
        long durationMs = endMs - startMs;
        double durationSec = durationMs / 1000.0;
        long tps = (long) (successfulOps / durationSec);
        double avgBatchLatencyMs = totalBatches > 0 ? (double) durationMs / totalBatches : 0.0;

        System.out.println("\n==================================================");
        System.out.println("   ROCKSDB DISTRIBUTED BENCHMARK (2 API SERVERS) ");
        System.out.println("==================================================");
        System.out.println("Batch Size        : " + BATCH_SIZE + " ops/batch");
        System.out.println("Total Threads     : " + TOTAL_THREADS + " (" + THREADS_PER_SERVER + " per API server)");
        System.out.println("API Server 1      : 192.168.0.211:" + API_PROXY_PORT);
        System.out.println("API Server 2      : 192.168.0.130:" + API_PROXY_PORT);
        System.out.println("----- Throughput ---------------------------------");
        System.out.println("Total Ops         : " + totalOps);
        System.out.println("Successful Ops    : " + successfulOps);
        System.out.println("Failed Ops        : " + failedOps);
        System.out.println("TPS               : " + tps + " ops/sec");
        System.out.printf("Throughput Lat    : %.4f ms/batch%n", avgBatchLatencyMs);
        System.out.println("----- Latency: API Server #1 (Laptop 1) ---------");
        System.out.printf("  Min             : %.4f ms%n", lat1[0]);
        System.out.printf("  Avg             : %.4f ms%n", lat1[1]);
        System.out.printf("  P50 (median)    : %.4f ms%n", lat1[2]);
        System.out.printf("  P95             : %.4f ms%n", lat1[3]);
        System.out.printf("  P99             : %.4f ms%n", lat1[4]);
        System.out.println("----- Latency: API Server #2 (Laptop 3) ---------");
        System.out.printf("  Min             : %.4f ms%n", lat2[0]);
        System.out.printf("  Avg             : %.4f ms%n", lat2[1]);
        System.out.printf("  P50 (median)    : %.4f ms%n", lat2[2]);
        System.out.printf("  P95             : %.4f ms%n", lat2[3]);
        System.out.printf("  P99             : %.4f ms%n", lat2[4]);
        System.out.println("==================================================");
    }

    private static double[] runSerialLatencyTest(ApiServiceGrpc.ApiServiceStub asyncStub, String label)
            throws InterruptedException {
        final int WARMUP_SAMPLES = 100;
        final int MEASURE_SAMPLES = 1000;
        final int TOTAL_SAMPLES = WARMUP_SAMPLES + MEASURE_SAMPLES;

        List<Long> latenciesNs = new ArrayList<>(MEASURE_SAMPLES);
        Random rand = new Random();

        CountDownLatch streamDone = new CountDownLatch(1);
        StreamObserver<OperationBatch>[] reqHolder = new StreamObserver[1];
        final CountDownLatch[] responseSignal = { new CountDownLatch(1) };
        final boolean[] measuring = { false };
        final long[] sendTs = { 0L };

        reqHolder[0] = asyncStub.streamOperations(new StreamObserver<BatchResult>() {
            @Override
            public void onNext(BatchResult value) {
                if (measuring[0])
                    latenciesNs.add(System.nanoTime() - sendTs[0]);
                responseSignal[0].countDown();
            }

            @Override
            public void onError(Throwable t) {
                responseSignal[0].countDown();
                streamDone.countDown();
            }

            @Override
            public void onCompleted() {
                streamDone.countDown();
            }
        });

        for (int i = 0; i < TOTAL_SAMPLES; i++) {
            if (i == WARMUP_SAMPLES)
                measuring[0] = true;

            int userId = rand.nextInt(TOTAL_USERS);
            int chance = rand.nextInt(100);
            Operation.OpType type = (chance < 50) ? Operation.OpType.READ
                    : (chance < 80) ? Operation.OpType.UPDATE : Operation.OpType.DELETE;
            OperationBatch batch = OperationBatch.newBuilder()
                    .addOperations(Operation.newBuilder().setUserId(userId).setType(type).build()).build();

            responseSignal[0] = new CountDownLatch(1);
            sendTs[0] = System.nanoTime();
            reqHolder[0].onNext(batch);
            responseSignal[0].await(5, TimeUnit.SECONDS);
        }

        reqHolder[0].onCompleted();
        streamDone.await(10, TimeUnit.SECONDS);

        Collections.sort(latenciesNs);
        int n = latenciesNs.size();
        return new double[] {
                latenciesNs.get(0) / 1_000_000.0,
                latenciesNs.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0,
                latenciesNs.get((int) (n * 0.50)) / 1_000_000.0,
                latenciesNs.get((int) (n * 0.95)) / 1_000_000.0,
                latenciesNs.get((int) (n * 0.99)) / 1_000_000.0,
        };
    }
}
