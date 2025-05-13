package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.max;
import static java.util.Collections.emptyList;
import static org.mitre.disttree.Misc.combineLists;

import java.util.List;

import org.mitre.caasd.commons.ids.TimeId;

public class Ops {

    /**
     * A TreeOperation is an "uncompacted" (i.e., reducible) operation that needs to be executed to
     * change the state of a DistanceTree. A collection of N TreeOperation can be compacted to a Set
     * of M operations (where {@code M <= N}). This reduction is possible because "intermediate tree
     * states" are skipped and only the "final tree state" is written with an I/O operation.
     * <p>
     * DataStores do not support TreeOperation because it would yield highly inefficient I/O.
     */
    public sealed interface TreeOperation<K, V> permits CreateRoot, NodeOp, TupleOp {}

    /** Builds the very first root node around this tuple. */
    public record CreateRoot<K, V>(Tuple<K, V> firstTuple) implements TreeOperation<K, V> {}

    /**
     * A NodeOp indicates a NodeHeader will change. A NodeOp may increase a node's radius, add a
     * childNode, or increment the tuple count. Certain NodeOp's require leaf nodes OR inner nodes.
     *
     * @param node        The node that needs to be changed
     * @param newRadius   The radius after the increase has been applied (only applied new > prior)
     * @param newChildren The id's of new childNodes
     * @param newTuples   The number of new tuples (i.e., items in a Leaf Node)
     */
    public record NodeOp<K, V>(NodeHeader<K> node, double newRadius, List<TimeId> newChildren, int newTuples)
            implements TreeOperation<K, V> {

        /** @return A copy of this NodeHeader with the increased radius. */
        public NodeHeader<K> resultingHeader() {

            double rad = max(node.radius(), newRadius);
            int newN = node.numTuples() + newTuples;

            // meets NodeHeader's restriction about the leaf node's always having null lists
            List<TimeId> children = node.isLeafNode() ? null : combineLists(node.childNodes(), newChildren);

            // written this way to meet NodeHeader record restrictions about the leaf node's having null lists
            return new NodeHeader<>(node.id(), node.parent(), node.center(), rad, children, newN);
        }

        /** Create an TreeOp that increases the radius of this Node. */
        static <K, V> NodeOp<K, V> increaseRadiusOf(NodeHeader<K> node, double newRadius) {
            return new NodeOp<>(node, newRadius, emptyList(), 0);
        }

        /** Create an TreeOp that increases the tuple count of this (leaf) Node. */
        static <K, V> NodeOp<K, V> incrementTupleCount(NodeHeader<K> node) {
            checkArgument(node.isLeafNode());
            return new NodeOp<>(node, 0, emptyList(), 1);
        }

        /** Compact many NodeEditOp that mutate the same target into a single "combined" Op. */
        static <K, V> NodeOp<K, V> reduce(List<NodeOp<K, V>> opsWithCommonTarget) {
            return opsWithCommonTarget.stream().reduce((a, b) -> combine(a, b)).get();
        }

        /** @return An NodeOp that "merges" these two ops */
        static <K, V> NodeOp<K, V> combine(NodeOp<K, V> a, NodeOp<K, V> b) {
            checkArgument(a.node.equals(b.node));

            double newRadius = max(a.newRadius, b.newRadius);
            List<TimeId> allNewChildren = combineLists(a.newChildren, b.newChildren);
            int tupleCountIncrement = a.newTuples + b.newTuples;

            return new NodeOp<>(a.node, newRadius, allNewChildren, tupleCountIncrement);
        }
    }

    /**
     * Assigns a Tuple to a specific leaf node. Can be used to CREATE or MOVE a tuple from one
     * DataPage to another.
     *
     * @param node  The node where this Tuple goes (must be a leaf node)
     * @param tuple The Tuple
     */
    public record TupleOp<K, V>(NodeHeader<K> node, Tuple<K, V> tuple) implements TreeOperation<K, V> {

        public TupleOp {
            checkArgument(node.isLeafNode());
        }

        public TimeId pageId() {
            return node.id();
        }
    }
}
