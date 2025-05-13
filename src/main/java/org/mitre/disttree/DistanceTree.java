package org.mitre.disttree;

import static java.util.Objects.requireNonNull;
import static org.mitre.disttree.TreeConfig.ReadWriteMode.READ_ONLY;

import java.util.List;

import org.mitre.disttree.TreeConfig.ReadWriteMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DistanceTree is the public facing "view" of this library. Its API is designed to make it
 * convenient to: (1) add data to a tree and (2) search a tree.
 * <p>
 * The goal of DistanceTree is to make the suite of internal DistanceTree components feel like a
 * simple single threaded data-structure.
 */
public class DistanceTree<K, V> {

    static final Logger LOGGER = LoggerFactory.getLogger(DistanceTree.class);

    final InternalTree<K, V> tree;

    final ReadWriteMode readWriteMode;

    /** Create a DistanceTree using this configuration. */
    public DistanceTree(TreeConfig<K, V> config) {
        this(new InternalTree<>(config));
    }

    DistanceTree(InternalTree<K, V> tree) {
        // this constructor cannot be public because InternalTree is VERY intentionally not public
        requireNonNull(tree);
        this.tree = tree;
        this.readWriteMode = tree.config().readWriteMode;
    }

    /** Block while adding one Batch of data to the DistanceTree. */
    public void addBatch(Batch<K, V> batch) {
        if (readWriteMode == READ_ONLY) {
            throw new UnsupportedOperationException("Cannot add batch in READ_ONLY mode");
        }
        LOGGER.atTrace()
                .setMessage("Adding a new batch of {} tuples")
                .addArgument(batch.tuples().size())
                .log();

        var job = new InsertBatchJob<>(tree, batch);
        job.run();
    }

    /** Block while adding multiple Batch of data to the DistanceTree. */
    public void addBatches(List<Batch<K, V>> batches) {
        batches.forEach(t -> addBatch(t));
    }

    /** Prompt the tree to rebuild all leaf nodes in the tree (this is an expensive operation). */
    public void repackTree() {

        LOGGER.atTrace().setMessage("Repacking the entire tree").log();

        var job = new RepackTreeJob<>(tree);
        job.run();
    }

    /**
     * Perform a k-Nearest-Neighbors search where k = 1.
     *
     * @param searchKey The point-in-space from which the closest tuple is found
     *
     * @return The Key/Value Result with the minimum distance to the search key
     */
    public SearchResults<K, V> getClosest(K searchKey) {
        verifyCanSearch();
        return knnSearch(searchKey, 1);
    }

    /**
     * Perform a k-Nearest-Neighbors search with arbitrary k.
     *
     * @param k         The number of tuples to search for
     * @param searchKey The point-in-space from which the closest tuples are found
     *
     * @return A collection of n Key/Value Results with the smallest distances to the search key
     */
    public SearchResults<K, V> knnSearch(K searchKey, int k) {
        verifyCanSearch();
        var treeSearcher = new TreeSearcher<>(tree);
        return treeSearcher.getNClosest(searchKey, k);
    }

    /**
     * Perform a range query that find all Tuples within a fixed distance of the searchKey.
     *
     * @param searchKey The point-in-space from which the closest tuples are found
     * @param range     The distance below which all tuples are included in the output.
     *
     * @return A Result for all keys within this range of the key.
     */
    public SearchResults<K, V> rangeSearch(K searchKey, double range) {
        verifyCanSearch();
        var treeSearcher = new TreeSearcher<>(tree);
        return treeSearcher.getAllWithinRange(searchKey, range);
    }

    private void verifyCanSearch() {
        if (readWriteMode == ReadWriteMode.WRITE_ONLY) {
            throw new UnsupportedOperationException("Cannot run query in WRITE_ONLY mode");
        }
    }

    /** @return A DTO containing a summary of this Tree's size and shape. */
    public TreeStats treeStats() {
        return tree.treeStats();
    }

    /**
     * @return The number of times the Distance Metric provided at configuration time was executed.
     *     This data is valuable when measuring the efficiency of different tree configurations.
     */
    public long distMetricExecutionCount() {
        return tree.config().distMetric().numExecutions();
    }

    /**
     * Equivalent to treeIterator(true)
     *
     * @return a TreeIterator that throws ConcurrentModificationExceptions if a batch is added to
     *     the Tree.
     */
    public TreeIterator<K, V> treeIterator() {
        return new TreeIterator<>(tree);
    }

    /**
     * @param preventMutation Controls if ConcurrentModificationExceptions should be thrown when
     *                        mutation is detected.
     *
     * @return a TreeIterator that may throw ConcurrentModificationExceptions if a batch is added to
     *     the Tree.
     */
    public TreeIterator<K, V> treeIterator(boolean preventMutation) {
        return new TreeIterator<>(tree, preventMutation);
    }
}
