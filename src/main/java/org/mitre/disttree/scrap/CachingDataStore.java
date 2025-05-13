// package org.mitre.disttree;
//
// import static com.google.common.base.Preconditions.checkArgument;
// import static java.util.Objects.requireNonNull;
//
// import java.time.Duration;
//
// import org.mitre.caasd.commons.ids.TimeId;
//
// import com.github.benmanes.caffeine.cache.Caffeine;
// import com.github.benmanes.caffeine.cache.LoadingCache;
//
/// **
// * CachingDataStore decorates a DataStore with a cache for read operations. Write Operations are
// * not supported.
// */
// public class CachingDataStore implements DataStore {
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
//     * @param dataStore          A DataStore that performs I/O operations to read data
//     * @param maxHeaderCacheSize The max number of NodeHeaders that can be stored in the cache (this
//     *                           number should be higher because NodeHeaders are small, so we should
//     *                           cache them liberally).
//     * @param maxPageCacheSize   The max number of DataPages that can be stored in the cache (this
//     *                           number will be the primary contributor to memory usage . This value
//     *                           may need to change if the MetricTree keeps many values in each leaf
//     *                           node)
//     */
//    public CachingDataStore(DataStore dataStore, int maxHeaderCacheSize, int maxPageCacheSize) {
//        requireNonNull(dataStore);
//        checkArgument(maxHeaderCacheSize > 0);
//        checkArgument(maxPageCacheSize > 0);
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
//                .maximumSize(maxPageCacheSize)
//                .expireAfterWrite(Duration.ofMinutes(5))
//                .refreshAfterWrite(Duration.ofMinutes(1))
//                .build(id -> loadPageFromSource(id));
//    }
//
//    /** Wrap a DataStore with a cache that will store 5000 nodes worth of data. */
//    public CachingDataStore(DataStore inner) {
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
//        throw new UnsupportedOperationException("Write operations are not supported");
//    }
// }
