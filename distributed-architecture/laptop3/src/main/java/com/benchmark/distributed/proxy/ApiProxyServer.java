package com.benchmark.distributed.proxy;

import com.benchmark.distributed.grpc.*;
import com.benchmark.distributed.security.JwtServerInterceptor;
import com.benchmark.distributed.security.JwtUtil;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;

import javax.net.ssl.KeyManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Laptop 3 (192.168.0.130): API Proxy Server #2
 * Acts as the TLS Termination and JWT Authentication Gateway.
 * Forwards bidirectional streams to the RocksDB server on Laptop 4
 * (192.168.0.118).
 */
public class ApiProxyServer {
    private static final Logger logger = Logger.getLogger(ApiProxyServer.class.getName());

    // This server listens on port 50051 for incoming client connections (TLS)
    private static final int LISTEN_PORT = 50051;

    // Laptop 4: RocksDB Database Server
    private static final String DB_SERVER_HOST = "192.168.0.118";
    private static final int DB_SERVER_PORT = 50052;

    private Server server;
    private ManagedChannel dbChannel;

    private void start() throws Exception {
        // 1. Create a fast plaintext tunnel to Laptop 4 (internal LAN — no TLS needed
        // internally)
        dbChannel = ManagedChannelBuilder.forAddress(DB_SERVER_HOST, DB_SERVER_PORT)
                .usePlaintext()
                .build();

        DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub = DatabaseServiceGrpc.newStub(dbChannel);

        // 2. Setup TLS for external clients (Laptop 2 — Benchmark Client)
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream("benchmark.p12")) {
            ks.load(fis, "password".toCharArray());
        }
        kmf.init(ks, "password".toCharArray());

        SslContext sslContext = GrpcSslContexts.configure(SslContextBuilder.forServer(kmf)).build();

        int coreCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(coreCount * 2);

        // 3. Start accepting TLS traffic from Laptop 2 (Benchmark Client)
        server = NettyServerBuilder.forPort(LISTEN_PORT)
                .sslContext(sslContext)
                .executor(executor)
                .addService(
                        ServerInterceptors.intercept(new ApiProxyServiceImpl(asyncDbStub), new JwtServerInterceptor()))
                .build()
                .start();

        logger.info("Laptop 3 — ApiProxyServer #2 started (TLS & JWT Enabled), listening on port " + LISTEN_PORT);
        logger.info("Forwarding operations to RocksDB Server (Laptop 4) at " + DB_SERVER_HOST + ":" + DB_SERVER_PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** Shutting down API Proxy Server #2 (Laptop 3)...");
            this.stop();
        }));
    }

    private void stop() {
        if (server != null)
            server.shutdown();
        if (dbChannel != null)
            dbChannel.shutdown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null)
            server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        final ApiProxyServer server = new ApiProxyServer();
        server.start();
        server.blockUntilShutdown();
    }

    /**
     * Proxy Implementation: Pure bidirectional stream forwarder.
     * Client → Proxy → RocksDB Server
     */
    static class ApiProxyServiceImpl extends ApiServiceGrpc.ApiServiceImplBase {
        private final DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub;

        ApiProxyServiceImpl(DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub) {
            this.asyncDbStub = asyncDbStub;
        }

        // --- JWT AUTHENTICATION HANDSHAKE ---
        @Override
        public void authenticate(AuthRequest request, StreamObserver<AuthResponse> responseObserver) {
            boolean success = "SuperSecretPassword123!".equals(request.getPassword());
            AuthResponse.Builder response = AuthResponse.newBuilder().setSuccess(success);

            if (success) {
                String token = JwtUtil.generateToken("benchmark-client");
                response.setToken(token);
                System.out.println("[Laptop 3] API Gateway issued JWT for: benchmark-client");
            }

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        // --- BIDIRECTIONAL STREAM FORWARDER ---
        @Override
        public StreamObserver<OperationBatch> streamOperations(StreamObserver<BatchResult> responseObserver) {
            // Establish outgoing stream: Proxy → RocksDB Server
            StreamObserver<OperationBatch> outgoingDbStream = asyncDbStub
                    .streamOperations(new StreamObserver<BatchResult>() {
                        @Override
                        public void onNext(BatchResult value) {
                            responseObserver.onNext(value);
                        }

                        @Override
                        public void onError(Throwable t) {
                            responseObserver.onError(t);
                        }

                        @Override
                        public void onCompleted() {
                            responseObserver.onCompleted();
                        }
                    });

            // Return incoming stream observer: Client → Proxy
            return new StreamObserver<OperationBatch>() {
                @Override
                public void onNext(OperationBatch value) {
                    outgoingDbStream.onNext(value);
                }

                @Override
                public void onError(Throwable t) {
                    outgoingDbStream.onError(t);
                }

                @Override
                public void onCompleted() {
                    outgoingDbStream.onCompleted();
                }
            };
        }
    }
}
