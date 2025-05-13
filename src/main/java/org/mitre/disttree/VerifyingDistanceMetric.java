package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Decorates a DistanceMetric, throws Exceptions when a distance is negative or NaN.
 *
 * @param <KEY>
 */
public class VerifyingDistanceMetric<KEY> implements DistanceMetric<KEY> {

    private final DistanceMetric<KEY> metric;

    /** Augment this DistanceMetric so that generating negative or NaN distance values causes RuntimeExceptions to be thrown. */
    public VerifyingDistanceMetric(DistanceMetric<KEY> metric) {
        requireNonNull(metric);
        this.metric = metric;
    }

    /**
     * Compute the distance btw two items, throw exceptions when DistanceMetric requirements are
     * broken.
     *
     * @param k1 The first of two items
     * @param k2 The second of two items
     *
     * @return The distance computed by the DistanceMetric provided at construction
     * @throws IllegalStateException if the distance was negative or NaN
     */
    @Override
    public double distanceBtw(KEY k1, KEY k2) {

        double dist = metric.distanceBtw(k1, k2);

        checkState(!Double.isNaN(dist), "A distance measurement was NaN.");
        checkState(dist >= 0, "A negative distance measurement was observed.");
        return dist;
    }

    public static <T> VerifyingDistanceMetric<T> verifyDistances(DistanceMetric<T> metric) {
        return new VerifyingDistanceMetric<>(metric);
    }
}
