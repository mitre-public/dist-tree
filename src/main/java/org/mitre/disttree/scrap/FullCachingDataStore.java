// package org.mitre.disttree;
//
// import static com.google.common.base.Preconditions.checkArgument;
// import static java.util.Objects.requireNonNull;
// import static java.util.stream.Collectors.toMap;
//
// import java.time.Duration;
// import java.util.List;
// import java.util.Map;
//
// import org.mitre.caasd.commons.ids.TimeId;
//
// import com.github.benmanes.caffeine.cache.Caffeine;
// import com.github.benmanes.caffeine.cache.LoadingCache;
//
/// **
// * FullCachingDataStore decorates a DataStore with a cache for both read and write operations.
// * <p>
// * All read operations are "read-through" (i.e., you access the cache first, then fall back to the
// * wrapped DataStore on a cache miss).
// * <p>
// * All Write operations are "write-through" (i.e., you write to the cache and the wrapped
// * DataStore)
// */
// public class FullCachingDataStore implements DataStore {
//
//    private static final int DEFAULT_CACHE_SIZE = 50000;
//
//    /** A DataStore that is usually launching I/O operations to read and store data. */
//    private final DataStore innerDataStore;
//
//    TimeId cachedRootId;
//
//    TimeId cachedLastTransactionId;
//
//    final LoadingCache<TimeId, NodeHeader<byte[]>> nodeCache;
//
//    final LoadingCache<TimeId, DataPage<byte[], byte[]>> pageCache;
//
//    /**
//     * @param dataStore            A DataStore that performs I/O operations to read and write data
//     * @param maxHeaderCacheSize   The max number of NodeHeaders that can be stored in the cache
//     *                             (this number should be higher because NodeHeaders are small, so
//     *                             we should cache them liberally).
//     * @param maxEntrySetCacheSize The max number of RoutedEntrySets that can be stored in the cache
//     *                             (this number will be the primary contributor to memory usage .
//     *                             This value may need ot change if the Metric Tree keeps many
//     *                             values in each leaf node)
//     */
//    public FullCachingDataStore(DataStore dataStore, int maxHeaderCacheSize, int maxEntrySetCacheSize) {
//        requireNonNull(dataStore);
//        checkArgument(maxHeaderCacheSize > 0);
//        checkArgument(maxEntrySetCacheSize > 0);
//
//        this.innerDataStore = dataStore;
//
//        this.cachedRootId = innerDataStore.rootId();
//        this.cachedLastTransactionId = innerDataStore.lastTransactionId();
//
//        this.nodeCache = Caffeine.newBuilder()
//                .maximumSize(maxHeaderCacheSize)
//                .expireAfterWrite(Duration.ofMinutes(5))
//                .refreshAfterWrite(Duration.ofMinutes(1))
//                .build(id -> loadNodeFromSource(id));
//
//        this.pageCache = Caffeine.newBuilder()
//                .maximumSize(maxEntrySetCacheSize)
//                .expireAfterWrite(Duration.ofMinutes(5))
//                .refreshAfterWrite(Duration.ofMinutes(1))
//                .build(id -> loadPageFromSource(id));
//    }
//
//    /** Wrap a DataStore with a cache that will store 5000 nodes worth of data. */
//    public FullCachingDataStore(DataStore inner) {
//        this(inner, DEFAULT_CACHE_SIZE, DEFAULT_CACHE_SIZE / 2);
//    }
//
//    private NodeHeader<byte[]> loadNodeFromSource(TimeId id) {
//        return innerDataStore.nodeAt(id);
//    }
//
//    private DataPage<byte[], byte[]> loadPageFromSource(TimeId id) {
//        return innerDataStore.dataPageAt(id);
//    }
//
//    @Override
//    public TimeId lastTransactionId() {
//        return cachedLastTransactionId;
//    }
//
//    @Override
//    public TimeId rootId() {
//        return cachedRootId;
//    }
//
//    @Override
//    public DataPage<byte[], byte[]> dataPageAt(TimeId id) {
//        return pageCache.get(id);
//    }
//
//    @Override
//    public NodeHeader<byte[]> nodeAt(TimeId id) {
//        return nodeCache.get(id);
//    }
//
//    @Override
//    public synchronized void applyTransaction(TreeTransaction<byte[], byte[]> transaction) {
//
//        /*
//         * This method IS IMPORTANT!
//         *
//         * How we implement writing data to the cache and I/O layer is important.  We want to keep
//         * the durable I/O copy synced with this in-memory copy.  The question is: "Do we prefer
//         * durability or speed?"  Should we require the slow and durable layer to "succeed" before
//         * we return?  Or can we return a CompletableFuture that tells us if we succeeded.
//         *
//         * Anyway -- for now we are using the "easy and correct" method
//         */
//
//        // Execute the I/O operations in the wrapped DataStore...
//        innerDataStore.applyTransaction(transaction);
//
//        // applying a transaction changes the rootId and lastTransactionId
//        cachedRootId = transaction.hasNewRoot() ? transaction.newRoot() : cachedRootId;
//        cachedLastTransactionId = transaction.transactionId();
//
//        // Purge everything that got completely in the transaction
//        nodeCache.invalidateAll(transaction.deletedNodeHeaders());
//        pageCache.invalidateAll(transaction.deletedLeafNodes());
//
//        // Save the new NodeHeaders because they are complete
//        nodeCache.putAll(asNodeMap(transaction.createdNodes()));
//        nodeCache.putAll(asNodeMap(transaction.updatedNodes()));
//
//        // Purge out-dated DataPages, CANNOT "directly put" pageUpdates because these pages are incomplete
//        //        pageCache.invalidateAll(pageIds(transaction.updatedPages()));  //@todo -- Determine the list of
// Pages
//        // that have been touched by this transaction
//        pageCache.invalidateAll();
//    }
//
//    private Map<TimeId, DataPage<byte[], byte[]>> asMap(List<DataPage<byte[], byte[]>> pages) {
//        return pages.stream().collect(toMap(res -> res.id(), res -> res));
//    }
//
//    private Map<TimeId, NodeHeader<byte[]>> asNodeMap(List<NodeHeader<byte[]>> headers) {
//        return headers.stream().collect(toMap(node -> node.id(), node -> node));
//    }
// }
