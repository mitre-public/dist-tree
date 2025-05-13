package org.mitre.disttree;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * A DataStore is a "Strategy Object" representing any I/O system that can read and write
 * NodeHeaders and DataPages.  One DataStore might utilize fast but non-durable In-Memory
 * data-storage.  A different DataStore may utilize a highly durable, but slower, data storage layer
 * like a NO-SQL database.
 * <p>
 * A DistanceTree uses a DataStore as a "dumb byte storage layer".  The DistanceTree uses its Serde,
 * tree configuration, and perhaps a cache to provide a facade around the DataStore.  The facade
 * makes reading and writing Key/Value data more natural.
 */
public interface DataStore {

    /**
     * @return The id of the last TreeTransaction that altered this tree. This id allows us to
     *     ensure TreeTransactions are "built from" the correct DistanceTree state.
     */
    TimeId lastTransactionId();

    /** @return the id of the root node. */
    TimeId rootId();

    /**
     * Get all the Entries stored at a specific leaf node in the Tree. This operation is analogous
     * to a disk I/O operation that retrieves a "page" or "block" of data from the B-Tree backing a
     * traditional database.
     *
     * @param id A unique ID that identifies this particular page of data. Inner nodes do not have
     *           Entries
     *
     * @return All the Entries stored at a specific leaf node in the Tree.
     * @throws NullPointerException When id is null
     */
    DataPage<byte[], byte[]> dataPageAt(TimeId id);

    /**
     * @return Basic information about the Node at this route.
     * @throws NullPointerException When id is null
     */
    NodeHeader<byte[]> nodeAt(TimeId id);

    /**
     * Perform I/O that "adds data" to a MetricTree. Ideally, this method will be ACID compliant
     * (i.e. all ops must succeed OR rollback everything)
     *
     * @param transaction A set of changes to a MetricTree that need to be executed successfully as
     *                    a transaction
     */
    // @todo -- Perhaps this signature should change to permit/encourage async writes
    void applyTransaction(TreeTransaction<byte[], byte[]> transaction);
}
