package org.mitre.disttree;

import static java.util.Objects.requireNonNull;

import java.util.ConcurrentModificationException;

/**
 * An InsertBatchJob attempts to mutate a DistanceTree when it is executed.
 */
class InsertBatchJob<K, V> implements Runnable {

    // This class feels like it could be a DistanceTree method. In fact, it used to be.
    // We refactored that implementation to use this "command style" approach to remove as much
    // mutation as possible from DistanceTree.

    final InternalTree<K, V> targetTree;
    final Batch<K, V> batch;

    /**
     * @param targetTree The DistanceTree we want to mutate
     * @param batch      The data we want to add
     */
    InsertBatchJob(InternalTree<K, V> targetTree, Batch<K, V> batch) {
        requireNonNull(batch);
        requireNonNull(targetTree);
        this.batch = batch;
        this.targetTree = targetTree;
    }

    @Override
    public void run() {
        // Should be a Callable that returns
        writeBatchToTree();
    }

    /**
     * Write a batch of data to the target DistanceTree. This batch will be converted to a single
     * transaction that updates the tree as efficiently as possible (i.e., only the aggregate result
     * is stored and intermediate state is skipped).
     */
    private void writeBatchToTree() {

        // @todo -- add mechanism to ensure batches & their transactions are threadsafe
        TransactionMaker<K, V> maker = new TransactionMaker<>(targetTree, batch);

        TreeTransaction<K, V> transaction = maker.computeTransaction();

        execute(transaction);
    }

    /** Update the underlying DataStore, make it represent a revised DistanceTree. */
    private void execute(TreeTransaction<K, V> transaction) {

        if (transaction.expectedTreeId() != targetTree.lastTransactionId()) {
            throw new ConcurrentModificationException();
        }

        var serdePair = targetTree.config().serdePair();
        var binaryTransaction = serdePair.serializeTransaction(transaction);

        targetTree.dataStore().applyTransaction(binaryTransaction);
    }
}
