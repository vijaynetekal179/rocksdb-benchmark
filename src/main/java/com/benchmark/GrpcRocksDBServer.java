package com.benchmark;

import com.benchmark.grpc.*;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;
import org.rocksdb.*;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.KeyManagerFactory;

public class GrpcRocksDBServer {
    private static final String DB_PATH = "./rocksdb-data";
    private static final int PORT = 9090;
    private static final int TOTAL_USERS = 1_000_000;
    private static final byte[][] CACHED_KEYS = new byte[TOTAL_USERS][];
    private static OptimisticTransactionDB db;

    static {
        RocksDB.loadLibrary();
        for (int i = 0; i < TOTAL_USERS; i++) {
            CACHED_KEYS[i] = ("user:" + i).getBytes();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Initializing GrpcRocksDBServer...");

        final Options options = buildOptions();
        db = OptimisticTransactionDB.open(options, DB_PATH);
        preSeedData(db);

        // Setup TLS from keystore
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream("benchmark.p12")) {
            ks.load(fis, "password".toCharArray());
        }
        kmf.init(ks, "password".toCharArray());

        SslContext sslContext = GrpcSslContexts.configure(
                SslContextBuilder.forServer(kmf)).build();

        // Scale thread pool to logical CPU count (12 threads on Ryzen 5 4600H → 24
        // threads)
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

        System.out.println("Secure gRPC Server is listening on port " + PORT);
        server.awaitTermination();
    }

    static class BenchmarkServiceImpl extends BenchmarkServiceGrpc.BenchmarkServiceImplBase {
        private final byte[] dummyValue = "updated_payload_data".getBytes();
        private final WriteOptions writeOpts = new WriteOptions().setDisableWAL(true);
        private final ReadOptions readOpts = new ReadOptions();

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
                    try (OptimisticTransactionOptions txnOptions = new OptimisticTransactionOptions()
                            .setSetSnapshot(true);
                            Transaction txn = db.beginTransaction(writeOpts, txnOptions)) {
                        for (Operation op : batch.getOperationsList()) {
                            byte[] key = CACHED_KEYS[op.getUserId()];
                            switch (op.getType()) {
                                case READ:
                                    db.get(readOpts, key);
                                    break;
                                case UPDATE:
                                    txn.put(key, dummyValue);
                                    break;
                                case DELETE:
                                    txn.delete(key);
                                    break;
                            }
                        }
                        txn.commit();
                        responseObserver.onNext(BatchResult.newBuilder().setSuccess(true).build());
                    } catch (RocksDBException e) {
                        // Conflict occurred, transaction cannot proceed
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

    private static Options buildOptions() {
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setMaxBackgroundJobs(6); // Use more background compaction threads
        options.setWriteBufferSize(64 * 1024 * 1024); // 64MB off-heap write buffer (reduced from 256MB)
        options.setMaxWriteBufferNumber(4); // 4 write buffers in memory (reduced from 6)
        options.setLevel0FileNumCompactionTrigger(4);
        options.setMaxBytesForLevelBase(256L * 1024 * 1024); // 256MB level base
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCache(new LRUCache(256L * 1024 * 1024)); // 256MB block cache (reduced from 2GB)
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        tableConfig.setBlockSize(16 * 1024); // 16KB block size for sequential reads
        options.setTableFormatConfig(tableConfig);
        return options;
    }

    private static void preSeedData(RocksDB db) throws RocksDBException {
        System.out.println("Pre-seeding data...");
        try (WriteOptions writeOpts = new WriteOptions().setDisableWAL(true);
                WriteBatch batch = new WriteBatch()) {
            for (int i = 0; i < TOTAL_USERS; i++) {
                batch.put(CACHED_KEYS[i], ("data_" + i).getBytes());
                if ((i + 1) % 10_000 == 0) {
                    db.write(writeOpts, batch);
                    batch.clear();
                }
            }
            if (batch.count() > 0)
                db.write(writeOpts, batch);
        }
        System.out.println("Pre-seeding complete.");
    }
}
