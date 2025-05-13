package org.mitre.disttree;

import static java.util.Objects.requireNonNull;

/**
 * Decorates a DistanceMetric, tracks how often the "distanceBtw" method is called.
 *
 * @param <KEY>
 */
public class CountingDistanceMetric<KEY> implements DistanceMetric<KEY> {

    private final DistanceMetric<KEY> metric;

    private long numExecutions;

    /** Augment this DistanceMetric so that we can track how often it is called. */
    public CountingDistanceMetric(DistanceMetric<KEY> metric) {
        requireNonNull(metric);
        this.metric = metric;
        this.numExecutions = 0;
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
        numExecutions++;
        return metric.distanceBtw(k1, k2);
    }

    /**
     * @return The number of times this DistanceMetric's distanceBtw method was called. This data
     *     is useful when measuring how much work is done during tree search and tree creation.
     */
    public long numExecutions() {
        return numExecutions;
    }

    /** @return The DistanceMetric provided at construction time. */
    public DistanceMetric<KEY> innerMetric() {
        return this.metric;
    }

    public static <T> CountingDistanceMetric<T> instrument(DistanceMetric<T> metric) {
        return new CountingDistanceMetric<>(metric);
    }
}
