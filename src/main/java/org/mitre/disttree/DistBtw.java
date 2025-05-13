package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.mitre.disttree.Ops.NodeOp.increaseRadiusOf;

import java.util.List;

import org.mitre.disttree.Ops.NodeOp;

/**
 * Stores the distance between a Key and the center of a particular node.
 * <p>
 * These records help reduce the number of times potentially expensive distance computation are
 * made.
 */
public record DistBtw<K>(NodeHeader<K> node, K key, double distance) {

    /** Measure the distance between a node's center point and this key. */
    public static <K> DistBtw<K> measureDistBtw(DistanceMetric<K> distMetric, NodeHeader<K> node, K key) {
        return new DistBtw<>(node, key, distMetric.distanceBtw(key, node.center()));
    }

    /** Given these Nodes, find the Node whose center is closest to the Key. */
    public static <K> DistBtw<K> chooseClosest(DistanceMetric<K> distMetric, List<NodeHeader<K>> options, K key) {
        requireNonNull(key);
        requireNonNull(options);
        checkState(!options.isEmpty());

        double minDist = Double.MAX_VALUE;
        NodeHeader<K> bestSoFar = null;

        for (NodeHeader<K> cur : options) {
            double dist = distMetric.distanceBtw(key, cur.center());
            if (dist < minDist) {
                minDist = dist;
                bestSoFar = cur;
            }
        }

        return new DistBtw<>(bestSoFar, key, minDist);
    }

    /** @return True if adding this Key to this node would increase its radius. */
    public boolean increasesRadius() {
        return distance > node.radius();
    }

    public NodeOp<K, ?> asRadiusOp() {
        return increaseRadiusOf(node, distance);
    }
}
