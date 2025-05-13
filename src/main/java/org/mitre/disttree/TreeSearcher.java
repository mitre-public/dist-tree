package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A TreeSearcher is a convenient, but non-public launch point for DistanceTree Searches.
 * <p>
 * Each call to one of these methods launches a "search process" whose performance depends on I/O
 * speed, cache hit rates, distance metric execution time, and the distribution of the data being
 * searched.
 *
 * @param <K>
 * @param <V>
 */
class TreeSearcher<K, V> {

    private final InternalTree<K, V> tree;

    TreeSearcher(InternalTree<K, V> tree) {
        requireNonNull(tree);
        this.tree = tree;
    }

    /**
     * Perform a k-Nearest-Neighbors search where k = 1.
     *
     * @param searchKey The point-in-space from which the closest tuple is found
     *
     * @return The Key/Value Result with the minimum distance to the search key
     */
    SearchResults<K, V> getClosest(K searchKey) {
        return getNClosest(searchKey, 1);
    }

    /**
     * Perform a k-Nearest-Neighbors search with arbitrary k.
     *
     * @param k         The number of tuples to search for
     * @param searchKey The point-in-space from which the closest tuples are found
     *
     * @return A collection of n Key/Value Results with the smallest distances to the search key
     */
    SearchResults<K, V> getNClosest(K searchKey, int k) {
        requireNonNull(searchKey);
        checkArgument(k >= 1, "n must be at least 1");

        Search<K, V> search = Search.knnSearch(searchKey, k, tree);
        search.executeQuery();

        return search.results();
    }

    /**
     * Perform a range query that find all Tuples within a fixed distance of the searchKey.
     *
     * @param searchKey The point-in-space from which the closest tuples are found
     * @param range     The distance below which all tuples are included in the output.
     *
     * @return A Result for all keys within this range of the key.
     */
    SearchResults<K, V> getAllWithinRange(K searchKey, double range) {

        requireNonNull(searchKey);
        checkArgument(range > 0, "The range must be strictly positive :{}", range);

        Search<K, V> search = Search.rangeSearch(searchKey, range, tree);
        search.executeQuery();

        return search.results();
    }
}
