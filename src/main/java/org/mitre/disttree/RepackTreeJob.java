package org.mitre.disttree;

import static java.util.Objects.requireNonNull;

import java.util.ConcurrentModificationException;

/**
 * A RepackTreeJob mutates a DistanceTree when it is executed.
 */
public class RepackTreeJob<K, V> implements Runnable {

    // This class feels like it could be a DistanceTree method. In fact, it used to be.
    // We refactored that implementation to use this "command style" approach to remove as much
    // mutation as possible from DistanceTree.

    final InternalTree<K, V> targetTree;

    /**
     * @param targetTree The DistanceTree we want to mutate
     */
    RepackTreeJob(InternalTree<K, V> targetTree) {
        requireNonNull(targetTree);
        this.targetTree = targetTree;
    }

    @Override
    public void run() {

        // @todo -- add mechanism to ensure batches & their transactions are threadsafe
        TransactionMaker<K, V> maker = new TransactionMaker<>(targetTree, null);
        TreeTransaction<K, V> transaction = maker.repackTree();

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
