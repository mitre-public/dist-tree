package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Stack;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * A TreeIterator "walks the tree" and provides the DataPages in the tree one at a time.
 *
 * @param <K> The Key type from the DistanceTree
 * @param <V> The Value type from the DistanceTree
 */
public class TreeIterator<K, V> implements Iterator<DataPage<K, V>> {

    /** The DistanceTree backing this Iterator. */
    private final InternalTree<K, V> tree;

    /** The last transactionId from the tree -- used to detect tree mutation. */
    private final TimeId expectedTreeId;

    /** When true tree mutations will cause ConcurrentModificationExceptions to be thrown. */
    private final boolean preventMutation;

    /** Contains Leaf and Inner Nodes that have not yet been processed. */
    private final Stack<NodeHeader<K>> nodesToTraverse;

    /** Construct a TreeIterator that prevents mutation. */
    TreeIterator(InternalTree<K, V> tree) {
        this(tree, true);
    }

    /** Construct a TreeIterator that may or may not prevent mutation. */
    TreeIterator(InternalTree<K, V> tree, boolean preventMutation) {
        requireNonNull(tree);
        this.tree = tree;
        this.expectedTreeId = tree.lastTransactionId();
        this.preventMutation = preventMutation;
        this.nodesToTraverse = new Stack<>();
        nodesToTraverse.push(tree.nodeAt(tree.rootId()));
    }

    @Override
    public boolean hasNext() {
        return !nodesToTraverse.isEmpty();
    }

    @Override
    public DataPage<K, V> next() {
        detectMutation();
        return findNextPage();
    }

    private DataPage<K, V> findNextPage() {

        while (true) {

            NodeHeader<K> top = nodesToTraverse.pop();

            if (top.isLeafNode()) {
                return tree.dataPageAt(top.id());
            } else {
                // top is an innerNode -- It should have children (otherwise it is malformed)
                var childNodes =
                        top.childNodes().stream().map(id -> tree.nodeAt(id)).toList();
                childNodes.forEach(child -> nodesToTraverse.push(child));

                checkState(!nodesToTraverse.isEmpty(), "Stack should never be empty");
            }
        }
    }

    private void detectMutation() {

        if (preventMutation && !expectedTreeId.equals(tree.lastTransactionId())) {
            throw new ConcurrentModificationException("DistanceTree has changed");
        }
    }
}
