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
 * Laptop 1: Benchmark Client
 * Restored the 96 thread / 200 batch Operations network bomber mechanism!
 */
public class BenchmarkClient {
    private static String apiProxyHost = "192.168.0.211";
    private static final int API_PROXY_PORT = 50051;

    private static final int TOTAL_USERS = 1_000_000;
    private static final int BATCH_SIZE = 200;
    // Massive thread load matches the single machine architecture test
    private static final int THREADS = 96;

    // Metadata key matching the server interceptor
    private static final Metadata.Key<String> AUTH_HEADER = Metadata.Key.of("authorization",
            Metadata.ASCII_STRING_MARSHALLER);

    public static void main(String[] args) throws Exception {
        if (args.length > 0)
            apiProxyHost = args[0];

        System.out.println("Starting Secure gRPC Benchmark Client (Laptop 1)");
        System.out.println("- Target: " + apiProxyHost + ":" + API_PROXY_PORT);

        // --- PROPER TLS: Load sever cert ---
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

        ManagedChannel channel = NettyChannelBuilder.forAddress(apiProxyHost, API_PROXY_PORT)
                .sslContext(sslContext)
                .overrideAuthority("localhost")
                .build();

        ApiServiceGrpc.ApiServiceBlockingStub blockingStub = ApiServiceGrpc.newBlockingStub(channel);

        // --- AUTHENTICATION HANDSHAKE WITH API PROXY ---
        AuthResponse authResponse = blockingStub.authenticate(
                AuthRequest.newBuilder().setPassword("SuperSecretPassword123!").build());

        if (!authResponse.getSuccess()) {
            System.err.println("Failed to authenticate to API Proxy Server.");
            channel.shutdown();
            return;
        }

        String jwtToken = authResponse.getToken();
        System.out.println("Authenticated successfully. JWT token received.");

        // Attach JWT to all subsequent calls via metadata interceptor
        Metadata authMetadata = new Metadata();
        authMetadata.put(AUTH_HEADER, "Bearer " + jwtToken);
        ApiServiceGrpc.ApiServiceStub asyncStub = ApiServiceGrpc.newStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(authMetadata));

        // --- PHASE 1: Serial Latency Test ---
        double[] latencyStats = runSerialLatencyTest(asyncStub);

        // --- PHASE 2: Throughput Benchmark ---
        CountDownLatch latch = new CountDownLatch(THREADS);
        AtomicLong totalBatches = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);
        AtomicLong failedOps = new AtomicLong(0);

        long globalStartTime = System.currentTimeMillis();
        AtomicBoolean isRunning = new AtomicBoolean(true);

        System.out.println("\n🔥 Pumping huge batches identically to original script...");

        for (int t = 0; t < THREADS; t++) {
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
                                public void onError(Throwable tr) {
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
                            batchBuilder.addOperations(Operation.newBuilder().setUserId(userId).setType(type).build());
                        }

                        requestObserver.onNext(batchBuilder.build());

                        // Smart gRPC Flow Control to avoid choking network link
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
                    streamLatch.await(1, TimeUnit.MINUTES);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        // Let the benchmark run for 1 minute
        Thread.sleep(60000);
        isRunning.set(false);

        latch.await();
        long globalEndTime = System.currentTimeMillis();
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);

        long totalOps = successfulOps.get() + failedOps.get();
        printResults(globalStartTime, globalEndTime, totalOps, successfulOps.get(), failedOps.get(), totalBatches.get(),
                latencyStats);
    }

    private static void printResults(long startMs, long endMs, long totalOps, long successfulOps, long failedOps,
            long totalBatches, double[] lat) {
        long durationMs = endMs - startMs;
        double durationSec = durationMs / 1000.0;
        long tps = (long) (successfulOps / durationSec);

        double avgBatchLatencyMs = totalBatches > 0 ? (double) durationMs / totalBatches : 0.0;

        System.out.println("\n==================================================");
        System.out.println(" ROCKSDB DISTRIBUTED BENCHMARK (TLS & PROXY) ");
        System.out.println("==================================================");
        System.out.println("Batch Size     : " + BATCH_SIZE + " ops/batch");
        System.out.println("Threads        : " + THREADS);
        System.out.println("----- Throughput --------------------------------");
        System.out.println("Total Ops      : " + totalOps);
        System.out.println("Success Ops    : " + successfulOps);
        System.out.println("TPS            : " + tps + " ops/sec");
        System.out.printf("Throughput Lat : %.4f ms/batch %n", avgBatchLatencyMs);
        System.out.println("----- Per-Op Latency (serial, 1000 samples) ----");
        System.out.printf("  Min          : %.4f ms%n", lat[0]);
        System.out.printf("  Avg          : %.4f ms%n", lat[1]);
        System.out.printf("  P50 (median) : %.4f ms%n", lat[2]);
        System.out.printf("  P95          : %.4f ms%n", lat[3]);
        System.out.printf("  P99          : %.4f ms%n", lat[4]);
        System.out.println("==================================================");
    }

    private static double[] runSerialLatencyTest(ApiServiceGrpc.ApiServiceStub asyncStub) throws InterruptedException {
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
                latenciesNs.get(n - 1) / 1_000_000.0
        };
    }
}
