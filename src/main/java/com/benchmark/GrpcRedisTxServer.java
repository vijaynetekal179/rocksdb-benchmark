package com.benchmark;

import com.benchmark.grpc.*;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

import javax.net.ssl.KeyManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GrpcRedisTxServer {
    private static final int PORT = 9092; // Running on 9092 for MULTI/EXEC
    private static final int TOTAL_USERS = 1_000_000;
    private static final byte[][] CACHED_KEYS = new byte[TOTAL_USERS][];

    // Jedis pool for connection management
    private static JedisPool jedisPool;

    static {
        for (int i = 0; i < TOTAL_USERS; i++) {
            CACHED_KEYS[i] = ("user:" + i).getBytes();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Initializing GrpcRedisTxServer (MULTI/EXEC Transaction Mode)...");

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // Set Max Total connections to safely accommodate 96-128 concurrent threads
        int corePoolSize = Runtime.getRuntime().availableProcessors() * 8;
        poolConfig.setMaxTotal(corePoolSize + 50);
        poolConfig.setMaxIdle(corePoolSize);
        poolConfig.setMinIdle(32);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);

        jedisPool = new JedisPool(poolConfig, "localhost", 6379, 180000); // 3-minute timeout for heavy TX loads

        try (Jedis jedis = jedisPool.getResource()) {
            if ("PONG".equals(jedis.ping())) {
                System.out.println("Successfully connected to Redis Server.");
            }
        } catch (Exception e) {
            System.err.println("FATAL: Cannot connect to Redis Server. Ensure it is running on localhost:6379");
            System.exit(1);
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream("benchmark.p12")) {
            ks.load(fis, "password".toCharArray());
        }
        kmf.init(ks, "password".toCharArray());

        SslContext sslContext = GrpcSslContexts.configure(
                SslContextBuilder.forServer(kmf)).build();

        int coreCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(coreCount * 2);
        System.out
                .println("Server executor: " + (coreCount * 2) + " threads (" + coreCount + " logical CPUs detected)");

        Server server = NettyServerBuilder.forPort(PORT)
                .sslContext(sslContext)
                .executor(executor)
                .addService(ServerInterceptors.intercept(new BenchmarkServiceImpl().bindService(),
                        new JwtServerInterceptor()))
                .build()
                .start();

        System.out.println("Secure gRPC Server (Redis Backend - Tx Mode) is listening on port " + PORT);
        server.awaitTermination();
    }

    static class BenchmarkServiceImpl extends BenchmarkServiceGrpc.BenchmarkServiceImplBase {
        private final byte[] dummyValue = "updated_payload_data".getBytes();

        @Override
        public void authenticate(AuthRequest request,
                StreamObserver<AuthResponse> responseObserver) {
            boolean success = "SuperSecretPassword123!".equals(request.getPassword());
            AuthResponse.Builder response = AuthResponse.newBuilder().setSuccess(success);
            if (success) {
                String token = JwtUtil.generateToken("benchmark-client");
                response.setToken(token);
                System.out.println("JWT issued for: benchmark-client");
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<OperationBatch> streamOperations(
                StreamObserver<BatchResult> responseObserver) {
            return new StreamObserver<OperationBatch>() {
                @Override
                public void onNext(OperationBatch batch) {
                    try (Jedis jedis = jedisPool.getResource()) {
                        // Use MULTI/EXEC to execute operations as an Atomic Transaction
                        Transaction tx = jedis.multi();

                        for (Operation op : batch.getOperationsList()) {
                            byte[] key = CACHED_KEYS[op.getUserId()];
                            switch (op.getType()) {
                                case READ:
                                    tx.get(key);
                                    break;
                                case UPDATE:
                                    tx.set(key, dummyValue);
                                    break;
                                case DELETE:
                                    tx.del(key);
                                    break;
                            }
                        }

                        // Execute all commands atomically
                        tx.exec();
                        responseObserver.onNext(BatchResult.newBuilder().setSuccess(true).build());

                    } catch (Exception e) {
                        e.printStackTrace();
                        responseObserver.onNext(BatchResult.newBuilder().setSuccess(false).build());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Client error: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
