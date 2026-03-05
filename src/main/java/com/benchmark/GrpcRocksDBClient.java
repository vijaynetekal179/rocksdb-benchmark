package com.benchmark;

import com.benchmark.grpc.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.ClientCallStreamObserver;

import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import javax.net.ssl.TrustManagerFactory;

public class GrpcRocksDBClient {
    private static final String SERVER_IP = "127.0.0.1"; // Change to 192.168.0.212 when running client remotely
    private static final int SERVER_PORT = 9090;

    private static final int TOTAL_USERS = 1_000_000;
    private static final int THREADS = Runtime.getRuntime().availableProcessors() * 8; // 96 on 12-thread CPU
    private static final int BATCH_SIZE = 200;
    private static final int TOTAL_REQUESTS = 1_000_000;

    // Metadata key matching the server interceptor
    private static final Metadata.Key<String> AUTH_HEADER = Metadata.Key.of("authorization",
            Metadata.ASCII_STRING_MARSHALLER);

    public static void main(String[] args) throws Exception {
        System.out.println("Starting gRPC Benchmark Client...");

        // --- PROPER TLS: Load server's public certificate explicitly ---
        // This verifies we are talking to the REAL server (not an impersonator)
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate serverCert;
        try (FileInputStream fis = new FileInputStream("benchmark.crt")) {
            serverCert = cf.generateCertificate(fis);
        }

        // Create a TrustStore containing only our server's certificate
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null); // empty truststore
        trustStore.setCertificateEntry("benchmark-server", serverCert);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        SslContext sslContext = GrpcSslContexts.configure(
                SslContextBuilder.forClient().trustManager(tmf))
                .build();

        ManagedChannel channel = NettyChannelBuilder.forAddress(SERVER_IP, SERVER_PORT)
                .sslContext(sslContext)
                .overrideAuthority("localhost") // must match the CN in the certificate
                .build();

        BenchmarkServiceGrpc.BenchmarkServiceBlockingStub blockingStub = BenchmarkServiceGrpc.newBlockingStub(channel);

        // --- AUTHENTICATION HANDSHAKE ---
        AuthResponse authResponse = blockingStub.authenticate(
                AuthRequest.newBuilder().setPassword("SuperSecretPassword123!").build());
        if (!authResponse.getSuccess()) {
            System.err.println("Failed to authenticate to server.");
            channel.shutdown();
            return;
        }

        String jwtToken = authResponse.getToken();
        System.out.println("Authenticated successfully. JWT token received.");

        // Attach JWT to all subsequent calls via metadata interceptor
        Metadata authMetadata = new Metadata();
        authMetadata.put(AUTH_HEADER, "Bearer " + jwtToken);
        BenchmarkServiceGrpc.BenchmarkServiceStub asyncStub = BenchmarkServiceGrpc.newStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(authMetadata));

        CountDownLatch latch = new CountDownLatch(THREADS);
        AtomicLong totalLatencyNs = new AtomicLong(0); // TRUE round-trip latency accumulator
        AtomicLong totalBatches = new AtomicLong(0); // batches that received a response
        AtomicLong successfulOps = new AtomicLong(0);
        AtomicLong failedOps = new AtomicLong(0);

        long globalStartTime = System.currentTimeMillis();
        AtomicBoolean isRunning = new AtomicBoolean(true);

        for (int t = 0; t < THREADS; t++) {
            new Thread(() -> {
                try {
                    CountDownLatch streamLatch = new CountDownLatch(1);

                    // Per-stream queue of send timestamps.
                    // gRPC streaming is ORDERED — first response matches first send.
                    ConcurrentLinkedQueue<Long> sendTimes = new ConcurrentLinkedQueue<>();

                    StreamObserver<OperationBatch> requestObserver = asyncStub.streamOperations(
                            new StreamObserver<BatchResult>() {
                                @Override
                                public void onNext(BatchResult value) {
                                    // ⏱️ Measure true RTT: pop the send time for this response
                                    Long sentAt = sendTimes.poll();
                                    if (sentAt != null) {
                                        totalLatencyNs.addAndGet(System.nanoTime() - sentAt);
                                        totalBatches.incrementAndGet();
                                    }
                                    if (value.getSuccess()) {
                                        successfulOps.addAndGet(BATCH_SIZE);
                                    } else {
                                        failedOps.addAndGet(BATCH_SIZE);
                                    }
                                }

                                @Override
                                public void onError(Throwable t) {
                                    System.err.println("Stream error: " + t.getMessage());
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
                                    : (chance < 80) ? Operation.OpType.UPDATE
                                            : Operation.OpType.DELETE;
                            batchBuilder.addOperations(
                                    Operation.newBuilder().setUserId(userId).setType(type).build());
                        }

                        // ⏱️ Stamp send time just before dispatching
                        sendTimes.offer(System.nanoTime());
                        requestObserver.onNext(batchBuilder.build());

                        // Smart gRPC Flow Control: Wait if network buffer is full
                        ClientCallStreamObserver<OperationBatch> callObserver = (ClientCallStreamObserver<OperationBatch>) requestObserver;

                        while (isRunning.get() && !callObserver.isReady()) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
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

        // Let the benchmark run for 1 hour
        Thread.sleep(3600000); // 60 minutes * 60 seconds * 1000 milliseconds
        isRunning.set(false);

        latch.await();
        long globalEndTime = System.currentTimeMillis();
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);

        long totalOps = successfulOps.get() + failedOps.get();
        printResults(globalStartTime, globalEndTime, totalOps,
                successfulOps.get(), failedOps.get(),
                totalLatencyNs.get(), totalBatches.get());
    }

    private static void printResults(long startMs, long endMs, long totalOps,
            long successfulOps, long failedOps, long totalLatencyNs, long totalBatches) {
        long durationMs = endMs - startMs;
        double durationSec = durationMs / 1000.0;
        long tps = (long) (successfulOps / durationSec);

        // Avg RTT per batch: total nanoseconds / number of batches → convert to ms
        double avgBatchRttMs = totalBatches > 0
                ? ((double) totalLatencyNs / totalBatches) / 1_000_000.0
                : 0.0;
        // Avg RTT per operation: divide batch RTT by batch size
        double avgOpLatencyMs = totalBatches > 0
                ? ((double) totalLatencyNs / (totalBatches * BATCH_SIZE)) / 1_000_000.0
                : 0.0;

        System.out.println("\n=== ROCKSDB gRPC Network Benchmark Results ===");
        System.out.println("==============================================");
        System.out.println("Workload     : Mixed (Read/Update/Delete)");
        System.out.println("Total Ops    : " + totalOps);
        System.out.println("Success Ops  : " + successfulOps);
        System.out.println("Failed Ops   : " + failedOps);
        System.out.println("TPS          : " + tps + " ops/sec");
        System.out.printf("Avg Batch RTT: %.4f ms (Network + RocksDB Server)%n", avgBatchRttMs);
        System.out.printf("Avg Op RTT   : %.4f ms (Batch RTT / %d)%n", avgOpLatencyMs, BATCH_SIZE);
        System.out.println("==============================================");
        System.out.println("Batches Sent : " + totalBatches + " (Batch Size: " + BATCH_SIZE + ")");
    }
}
