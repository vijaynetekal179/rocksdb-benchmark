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
import java.io.IOException;
import java.security.KeyStore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Laptop 2: API Proxy Server
 * Acts as the TLS Termination and JWT Authentication Gateway.
 * Forwards bidirectional streams directly to the internal DB server.
 */
public class ApiProxyServer {
    private static final Logger logger = Logger.getLogger(ApiProxyServer.class.getName());

    private static final int LISTEN_PORT = 50051;
    private static String dbServerHost = "192.168.0.130";
    private static final int DB_SERVER_PORT = 50052;

    private Server server;
    private ManagedChannel dbChannel;

    private void start() throws Exception {
        // 1. Create a FAST UNSecured tunnel to Laptop 3 (Database Tier)
        dbChannel = ManagedChannelBuilder.forAddress(dbServerHost, DB_SERVER_PORT)
                .usePlaintext() // Inside the VPC, no TLS hop needed internally
                .build();

        DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub = DatabaseServiceGrpc.newStub(dbChannel);

        // 2. Setup TLS from keystore (Exposed externally to Laptop 1)
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream("benchmark.p12")) {
            ks.load(fis, "password".toCharArray());
        }
        kmf.init(ks, "password".toCharArray());

        SslContext sslContext = GrpcSslContexts.configure(SslContextBuilder.forServer(kmf)).build();

        int coreCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(coreCount * 2);

        // 3. Start accepting Secure Traffic from Laptop 1
        server = NettyServerBuilder.forPort(LISTEN_PORT)
                .sslContext(sslContext)
                .executor(executor)
                // Add JWT Security
                .addService(
                        ServerInterceptors.intercept(new ApiProxyServiceImpl(asyncDbStub), new JwtServerInterceptor()))
                .build()
                .start();

        logger.info("ApiProxyServer started (TLS & JWT Enabled), listening on " + LISTEN_PORT);
        logger.info("Streaming operations to DB Tier at " + dbServerHost + ":" + DB_SERVER_PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down API Proxy server");
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
        if (args.length > 0)
            dbServerHost = args[0];
        final ApiProxyServer server = new ApiProxyServer();
        server.start();
        server.blockUntilShutdown();
    }

    /**
     * Proxy Implementation: Acts as a pure hose streaming traffic between client
     * and server.
     */
    static class ApiProxyServiceImpl extends ApiServiceGrpc.ApiServiceImplBase {
        private final DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub;

        ApiProxyServiceImpl(DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub) {
            this.asyncDbStub = asyncDbStub;
        }

        // --- AUTHENTICATION HANDSHAKE LOGIC CATCHER ---
        @Override
        public void authenticate(AuthRequest request, StreamObserver<AuthResponse> responseObserver) {
            // Note: The original project had the DB doing JWT checks. In the distributed
            // model,
            // the Gateway (this Proxy) should check and issue the JWT, keeping the DB
            // completely oblivious of HTTP auth!
            boolean success = "SuperSecretPassword123!".equals(request.getPassword());
            AuthResponse.Builder response = AuthResponse.newBuilder().setSuccess(success);

            if (success) {
                String token = JwtUtil.generateToken("benchmark-client");
                response.setToken(token);
                System.out.println("API Gateway issued JWT for: benchmark-client");
            }

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        // --- BIDIRECTIONAL STREAM FORWARDER ---
        @Override
        public StreamObserver<OperationBatch> streamOperations(StreamObserver<BatchResult> responseObserver) {
            // 1. Establish an outgoing stream from Proxy -> Database
            StreamObserver<OperationBatch> outgoingDbStream = asyncDbStub
                    .streamOperations(new StreamObserver<BatchResult>() {
                        @Override
                        public void onNext(BatchResult value) {
                            // Send DB response back to Client
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

            // 2. Return an incoming stream observer to consume Client -> Proxy and pipe it
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
