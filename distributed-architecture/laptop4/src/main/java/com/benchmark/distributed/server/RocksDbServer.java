package com.benchmark.distributed.server;

import com.benchmark.distributed.grpc.*;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.rocksdb.*;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Laptop 4 (192.168.0.118): RocksDB Database Server
 * Listens on port 50052 (plaintext — internal LAN only).
 * Accepts concurrent connections from both API Proxy servers:
 * - Laptop 1 (192.168.0.211)
 * - Laptop 3 (192.168.0.130)
 * Optimized with OptimisticTransactionDB and WAL Disabled for batching speed.
 */
public class RocksDbServer {
    private static final String DB_PATH = "C:\\distributed-rocksdb-data";
    private static final int PORT = 50052;
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
        System.out.println("=================================================");
        System.out.println(" Laptop 4 — RocksDB Database Server              ");
        System.out.println(" IP: 192.168.0.118  |  Port: " + PORT + "            ");
        System.out.println(" Accepts connections from:                        ");
        System.out.println("   Laptop 1 (API Server #1): 192.168.0.211       ");
        System.out.println("   Laptop 3 (API Server #2): 192.168.0.130       ");
        System.out.println("=================================================");

        File dbDir = new File(DB_PATH);
        if (!dbDir.exists())
            dbDir.mkdirs();

        final Options options = buildOptions();
        db = OptimisticTransactionDB.open(options, DB_PATH);

        // Pre-seed 1M keys so READ operations don't fail during benchmark
        preSeedData(db);

        int coreCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(coreCount * 2);
        System.out.println("DB Server executor: " + (coreCount * 2) + " threads");

        // Bind to all interfaces (0.0.0.0) so both API servers can connect
        Server server = NettyServerBuilder.forPort(PORT)
                .executor(executor)
                .addService(new DatabaseServiceImpl())
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build()
                .start();

        System.out.println("RocksDB gRPC Server is listening on 0.0.0.0:" + PORT);
        server.awaitTermination();
    }

    static class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
        private final byte[] dummyValue = "updated_payload_data_distributed_tier_rocksdb123".getBytes();
        private final WriteOptions writeOpts = new WriteOptions().setDisableWAL(true);
        private final ReadOptions readOpts = new ReadOptions();

        @Override
        public StreamObserver<OperationBatch> streamOperations(StreamObserver<BatchResult> responseObserver) {
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
                                default:
                                    break;
                            }
                        }
                        txn.commit();
                        responseObserver.onNext(BatchResult.newBuilder().setSuccess(true).build());
                    } catch (RocksDBException e) {
                        responseObserver.onNext(BatchResult.newBuilder().setSuccess(false).build());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("DB Transaction Error: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }

    private static Options buildOptions() {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        System.out.println("[RocksDB] Detected " + cpuCores + " CPU cores. Enabling full C++ parallelism...");

        Options options = new Options();

        // LAYER 1: OS-level C++ thread pools inside RocksDB
        options.setIncreaseParallelism(cpuCores);

        // LAYER 2: Allow a single compaction job to split into parallel sub-tasks
        options.setMaxSubcompactions(cpuCores);

        // LAYER 3: Tune write buffers and compaction for write-heavy workloads
        options.optimizeLevelStyleCompaction();

        options.setCreateIfMissing(true);
        options.setMaxBackgroundJobs(cpuCores);
        options.setWriteBufferSize(64 * 1024 * 1024);
        options.setMaxWriteBufferNumber(4);
        options.setLevel0FileNumCompactionTrigger(4);
        options.setMaxBytesForLevelBase(256L * 1024 * 1024);

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCache(new LRUCache(256L * 1024 * 1024));
        tableConfig.setFilterPolicy(new BloomFilter(10, false));
        tableConfig.setBlockSize(16 * 1024);
        options.setTableFormatConfig(tableConfig);

        return options;
    }

    private static void preSeedData(RocksDB db) throws RocksDBException {
        System.out.println("Pre-seeding 1M keys...");
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
