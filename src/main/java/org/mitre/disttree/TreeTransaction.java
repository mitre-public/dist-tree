package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.nonNull;
import static org.mitre.caasd.commons.ids.TimeId.newId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * A TreeTransaction is a collection of changes to NodeHeaders and TupleAssignment that will convert
 * a DistanceTree from one "valid tree state" a new "valid tree state" -- Assuming, all changes are
 * successfully applied at the storage layer
 */
public class TreeTransaction<K, V> {

    /** An identifier for this Transaction, used by DistanceTrees to track changes. */
    private final TimeId transactionId;

    /**
     * The transaction id of the last transaction to alter the target DistanceTree. This transaction
     * is only valid when the target tree's "lastTransactionId()" still matches this expected value.
     * If the target DistanceTree's lastTransactionId() does not match this value then we have
     * detected unwanted concurrent modifications and must throw out the mutations gathered inside
     * this transaction.
     */
    private final TimeId expectedTreeId;

    /** The NodeHeaders created during this transaction. */
    private final List<NodeHeader<K>> createdNodes;

    /** The NodeHeader mutated (e.g., bigger radius, more children) during this transaction. */
    private final List<NodeHeader<K>> updatedNodes;

    /** The Tuples created during this transaction. */
    private final List<TupleAssignment<K, V>> createdTuples;

    /** The Tuples moved during this transaction. */
    private final List<TupleAssignment<K, V>> updatedTuples;

    /**
     * These are the ids of DataPages that need to be removed because all their tuples were moved to
     * other DataPage during a split or repacking operation.
     */
    private final Set<TimeId> deletedPages;

    private final Set<TimeId> deletedNodes;

    private final TimeId newRoot;

    /**
     * Collect the "IO ops" needed to update the DistanceTree's state. A "new root" is detected if
     * one of the incoming NodeHeaders has no parent.
     *
     * @param expectedTreeId The "lastTransactionId" of the target DistanceTree
     * @param createdNodes   The NodeHeaders created during this transaction
     * @param updatedNodes   The NodeHeader mutated (e.g., bigger radius, more children) during this
     *                       transaction
     * @param createdTuples  The Tuples created during this transaction
     * @param updatedTuples  The Tuples moved from on page to another during this transaction
     * @param deletedPages   These ids of DataPages that need to have their preexisting data removed
     *                       because all their tuples were moved to a different DataPage during a
     *                       split or repacking operation
     * @param deletedNodes   These ids of NodeHeaders that need to be deleted (not merely replaced)
     */
    TreeTransaction(
            TimeId expectedTreeId,
            List<NodeHeader<K>> createdNodes,
            List<NodeHeader<K>> updatedNodes,
            List<TupleAssignment<K, V>> createdTuples,
            List<TupleAssignment<K, V>> updatedTuples,
            Set<TimeId> deletedPages,
            Set<TimeId> deletedNodes) {

        this.transactionId = newId();
        this.expectedTreeId = expectedTreeId;

        this.createdNodes = createdNodes;
        this.updatedNodes = updatedNodes;

        this.createdTuples = createdTuples;
        this.updatedTuples = updatedTuples;

        this.deletedPages = deletedPages;
        this.deletedNodes = deletedNodes;

        verifyDistinctIds(createdNodes, updatedNodes);

        this.newRoot = findNewRoot(createdNodes, updatedNodes);
    }

    private static <K> void verifyDistinctIds(List<NodeHeader<K>> created, List<NodeHeader<K>> updated) {

        List<NodeHeader<K>> allNodes = new ArrayList<>();
        allNodes.addAll(created);
        allNodes.addAll(updated);

        long numDistinctIds = allNodes.stream().map(NodeHeader::id).distinct().count();

        checkArgument(allNodes.size() == numDistinctIds, "NodeHeader cannot be created AND updated");
    }

    /**
     * If one of these NodeHeaders is a root node, find its TimeId.  Fail if 2+ roots nodes are
     * found.
     *
     * @return The TimeId of the root node, or null when no NodeHeader's represent a root node.
     */
    private static <K> TimeId findNewRoot(List<NodeHeader<K>> created, List<NodeHeader<K>> updated) {

        List<NodeHeader<K>> roots = new ArrayList<>();
        roots.addAll(created.stream().filter(node -> node.isRoot()).toList());
        roots.addAll(updated.stream().filter(node -> node.isRoot()).toList());

        if (roots.size() > 1) {
            throw new IllegalStateException("CANNOT add multiple root nodes");
        }

        return roots.isEmpty() ? null : roots.get(0).id();
    }

    public TimeId expectedTreeId() {
        return this.expectedTreeId;
    }

    public TimeId transactionId() {
        return this.transactionId;
    }

    /** These are the Nodes that are created during this transaction (i.e., CRUD's Create) */
    public List<NodeHeader<K>> createdNodes() {
        return createdNodes;
    }

    /** These are the Nodes that are updated during this transaction (i.e., CRUD's Update) */
    public List<NodeHeader<K>> updatedNodes() {
        return updatedNodes;
    }

    /** @return The Tuples created during this transaction. */
    public List<TupleAssignment<K, V>> createdTuples() {
        return createdTuples;
    }

    /** @return The Tuples moved during this transaction. */
    public List<TupleAssignment<K, V>> updatedTuples() {
        return updatedTuples;
    }

    /**
     * These TimeIds correspond to DataPages that got split during this transaction. Thus, the
     * Tuples previously contained at these DataPages have been moved.  The DataPages with these
     * ids need to be deleted BEFORE adding the new entryUpdates.
     */
    public Set<TimeId> deletedLeafNodes() {
        return deletedPages;
    }

    /**
     * These TimeIds correspond to NodeHeaders that need to be deleted (not merely replaced or
     * updated). The need to remove NodeHeaders arises when a "repack" operation leaves DataPage and
     * Nodes without data.  This method allows us to remove "Orphan NodeHeaders".
     */
    public Set<TimeId> deletedNodeHeaders() {
        return deletedNodes;
    }

    /**
     * @return True when this transaction reassigns the root node (i.e., increases tree depth by 1)
     */
    public boolean hasNewRoot() {
        return nonNull(newRoot);
    }

    public TimeId newRoot() {
        return newRoot;
    }

    public String describe() {
        StringBuilder builder = new StringBuilder("This transaction will:\n");

        createdTuples()
                .forEach(
                        ta -> builder.append("  Create the tuple: " + ta.tuple().id() + " in " + ta.pageId()));
        updatedTuples()
                .forEach(tr -> builder.append("  Move the tuple: " + tr.tuple().id() + " to " + tr.pageId()));

        createdNodes().forEach(node -> builder.append("  Create the node: " + node.toString() + "\n"));
        updatedNodes().forEach(node -> builder.append("  Update the node: " + node.toString() + "\n"));

        deletedNodeHeaders().forEach(deleteId -> builder.append("  Delete the node at: " + deleteId + "\n"));

        return builder.toString();
    }
}
