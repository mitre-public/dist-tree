package org.mitre.disttree;

import static java.lang.Math.max;

import java.util.List;

/**
 * The DistanceMetric must define a true Metric Space (in the strict algebraic sense) for KEY
 * objects. This means the following should be true:
 * <p>
 * <pre>{@code
 * (1) d(x,y) >= 0
 * (2) d(x,y) = d(y,x)
 * (3) d(x,z) <= d(x,y) + d(y,z)
 * }</pre>
 */
@FunctionalInterface
public interface DistanceMetric<KEY> {

    /**
     * @param item1 The first of two items
     * @param item2 The second of two items
     *
     * @return The distance between the 2 objects in a Metric Space (this method must define a
     *     proper Metric Space in the strict algebraic sense).
     */
    double distanceBtw(KEY item1, KEY item2);

    /**
     * Use this DistanceMetric to compute the radius of the Sphere with the provided center that
     * "just barely contains" these all these items.
     *
     * @param center     The center of an "N-dimensional sphere"
     * @param otherItems Other items in the "N-dimensional sphere"
     */
    default double computeRadius(KEY center, List<KEY> otherItems) {

        if (otherItems.isEmpty()) {
            return 0;
        }

        return otherItems.stream()
                .mapToDouble(key -> distanceBtw(key, center))
                .max()
                .getAsDouble();
    }

    /**
     * Computes the "largest possible radius" a new inner node could have if new node has this
     * center and contains these childNodes.  This method very-likely overestimates the radius, but
     * this computation does not require "touching" all the tuples in the child nodes.
     * Additionally, this overestimation is "less bad" because the radius measurements at the leaf
     * nodes are always 100% correct.
     */
    default double estimateInnerNodeRadius(KEY center, List<NodeHeader<KEY>> childNodes) {

        double curRadius = 0;
        for (NodeHeader<KEY> node : childNodes) {
            double centerToCenterDist = distanceBtw(center, node.center());
            double fullDist = centerToCenterDist + node.radius();
            curRadius = max(curRadius, fullDist);
        }
        return curRadius;
    }
}
