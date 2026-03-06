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
 * Laptop 3: Database Server
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
        System.out.println("Initializing GrpcRocksDBServer (Distributed Tier)...");

        // Prepare Database Dir
        File dbDir = new File(DB_PATH);
        if (!dbDir.exists())
            dbDir.mkdirs();

        final Options options = buildOptions();
        db = OptimisticTransactionDB.open(options, DB_PATH);

        // Seed the 1,000,000 keys so READ operations don't fail during benchmark
        preSeedData(db);

        // Scale thread pool to logical CPU count for the massive parallel writes
        int coreCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(coreCount * 2);
        System.out.println("DB Server executor: " + (coreCount * 2) + " threads");

        // Internal Server - NO TLS needed since it's only talking to the Proxy inside
        // the private network.
        Server server = NettyServerBuilder.forPort(PORT)
                .executor(executor)
                .addService(new DatabaseServiceImpl())
                // Huge max inbound to handle the large batches
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build()
                .start();

        System.out.println("Fast Internal DB gRPC Server is listening on port " + PORT);
        server.awaitTermination();
    }

    static class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
        private final byte[] dummyValue = "updated_payload_data_distributed_tier_rocksdb123".getBytes();
        // Massively speeds up TPS by bypassing disk network sync locking
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

        // LAYER 1: Spin up OS-level C++ thread pools inside RocksDB
        // Source: options.cc line 694 - sets env->SetBackgroundThreads(n, LOW/HIGH)
        options.setIncreaseParallelism(cpuCores);

        // LAYER 2: Allow a single compaction job to split into parallel sub-tasks
        // Source: options.h line 924 - default is 1 (no sub-compactions!)
        options.setMaxSubcompactions(cpuCores);

        // LAYER 3: Tune write buffers and compaction style for write-heavy workloads
        options.optimizeLevelStyleCompaction();

        options.setCreateIfMissing(true);
        // Match background job count to CPU core count (previously hardcoded to 8)
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
