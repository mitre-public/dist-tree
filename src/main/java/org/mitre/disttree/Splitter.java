package org.mitre.disttree;

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * A Splitter hides the implementation responsible for selecting the center point of new Nodes in
 * the Tree.
 */
public class Splitter<K, V> {

    private final DistanceMetric<K> distMetric;

    private final CenterSelector<K> centerSelectorStrategy;

    public Splitter(DistanceMetric<K> metric) {
        this(metric, CenterSelectors.maxOfRandomSamples());
    }

    public Splitter(DistanceMetric<K> metric, CenterSelector<K> strategy) {
        this.distMetric = requireNonNull(metric);
        this.centerSelectorStrategy = requireNonNull(strategy);
    }

    public List<K> split(List<K> keys) {
        return centerSelectorStrategy.selectCenterPoints(keys, distMetric);
    }

    public record SplitResult<K, V>(Stub<K, V> left, Stub<K, V> right) {}

    /** A Stub provides enough info to make a NodeHeader and DataPage for each Split. */
    public record Stub<K, V>(K center, Set<Tuple<K, V>> tuples, double radius) {}

    /**
     * Split a DataPage that is too big to be "legally" retain in the tree.
     *
     * @return A SplitResult which contains the stubs of the new DataPages.
     */
    public SplitResult<K, V> splitCarefully(DataPage<K, V> overflowingNode) {

        List<K> newCenters = centerSelectorStrategy.selectCenterPoints(overflowingNode.keyList(), distMetric);

        /*
         * This helper record gathers the info we need to correctly, and efficiently, choose which
         * child node the tuples in the overflowingNode should be assigned to.
         */
        record DistanceInfoPair<K, V>(Tuple<K, V> tuple, double leftDist, double rightDist) {}

        // Collect the distance info we need....
        List<DistanceInfoPair<K, V>> distInfo = overflowingNode.tuples().stream()
                .map(tuple -> new DistanceInfoPair<>(
                        tuple,
                        distMetric.distanceBtw(newCenters.get(0), tuple.key()),
                        distMetric.distanceBtw(newCenters.get(1), tuple.key())))
                .toList();

        // Now that we have the info we need create the components of left and right child
        TreeSet<Tuple<K, V>> leftTuples = new TreeSet<>();
        TreeSet<Tuple<K, V>> rightTuples = new TreeSet<>();
        double leftRadius = 0;
        double rightRadius = 0;
        boolean tieBreaker = false;

        for (DistanceInfoPair<K, V> distPair : distInfo) {

            if (distPair.leftDist == distPair.rightDist) {
                // use the tiebreaker when distances are equal
                if (tieBreaker) {
                    leftTuples.add(distPair.tuple);
                    leftRadius = max(leftRadius, distPair.leftDist);
                } else {
                    rightTuples.add(distPair.tuple);
                    rightRadius = max(rightRadius, distPair.rightDist);
                }
                tieBreaker = !tieBreaker; // alternate the tiebreaker

            } else if (distPair.leftDist < distPair.rightDist) {
                leftTuples.add(distPair.tuple);
                leftRadius = max(leftRadius, distPair.leftDist);
            } else {
                rightTuples.add(distPair.tuple);
                rightRadius = max(rightRadius, distPair.rightDist);
            }
        }

        Stub<K, V> left = new Stub<>(newCenters.get(0), leftTuples, leftRadius);
        Stub<K, V> right = new Stub<>(newCenters.get(1), rightTuples, rightRadius);

        return new SplitResult<>(left, right);
    }

    /**
     * QUICKLY!!! Split a DataPage that is too big to be "legally" kept in the tree.  This method
     * yields results that must be immediately repacked.  The optimization here is that we know
     * these repacks are coming -- so there is no point wasting compute time finding "optimal
     * splits" when the work will immediately be thrown away.
     *
     * @return A SplitResult which contains the suboptimal stubs of the new DataPages.
     */
    public SplitResult<K, V> quickThrowAwaySplit(DataPage<K, V> overflowingNode) {

        List<K> newCenters = centerSelectorStrategy.selectCenterPoints(overflowingNode.keyList(), distMetric);

        TreeSet<Tuple<K, V>> leftTuples = new TreeSet<>();
        TreeSet<Tuple<K, V>> rightTuples = new TreeSet<>();
        boolean tieBreaker = false;

        for (Tuple<K, V> tuple : overflowingNode.tuples()) {

            Set<Tuple<K, V>> target = tieBreaker ? leftTuples : rightTuples;
            target.add(tuple);

            tieBreaker = !tieBreaker; // alternate the tiebreaker
        }

        Stub<K, V> left = new Stub<>(newCenters.get(0), leftTuples, 0);
        Stub<K, V> right = new Stub<>(newCenters.get(1), rightTuples, 0);

        return new SplitResult<>(left, right);
    }
}
