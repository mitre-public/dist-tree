package org.mitre.disttree;

import java.util.List;
import java.util.Random;

public class CenterSelectors {

    public static <K> CenterSelector<K> maxOfRandomSamples() {
        return new RandomizedMaxDistanceSelector<>();
    }

    /**
     * This CenterSelector picks multiple random Keys Pairs from a List of Keys provided and returns
     * the pair with the largest distance between them. This pair of Keys should generate 2 child
     * spheres whose volumes overlap as little as possible.
     */
    private static class RandomizedMaxDistanceSelector<K> implements CenterSelector<K> {

        final Random rng = new Random(17L);

        /**
         * @param keys   A List of Keys that needs to be split
         * @param metric The distance metric that measures distance between 2 keys
         *
         * @return Two keys that will be used as the centerPoints for two new Spheres
         */
        @Override
        public List<K> selectCenterPoints(List<K> keys, DistanceMetric<K> metric) {

            int numPairsToDraw = (int) Math.sqrt(keys.size()); // sqrt strikes a good balance

            List<K> bestPair = selectRandomPairOfKeys(keys, rng);
            double biggestDistance = metric.distanceBtw(bestPair.get(0), bestPair.get(1));
            numPairsToDraw--;

            for (int i = 0; i < numPairsToDraw; i++) {

                List<K> newPair = selectRandomPairOfKeys(keys, rng);
                double newDistance = metric.distanceBtw(newPair.get(0), newPair.get(1));

                if (newDistance > biggestDistance) {
                    bestPair = newPair;
                    biggestDistance = newDistance;
                }
            }

            return bestPair;
        }
    }

    private static <KEY> List<KEY> selectRandomPairOfKeys(List<KEY> keys, Random rng) {

        // pick 2 random -- and unique -- index values
        int n = keys.size();
        int index1 = rng.nextInt(n);
        int index2 = rng.nextInt(n);
        while (index1 == index2) {
            index2 = rng.nextInt(n);
        }

        return List.of(keys.get(index1), keys.get(index2));
    }
}
