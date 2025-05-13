package org.mitre.disttree;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * A SearchResult is a single "result record" for reporting the output of a K-nearest neighbor
 * search or a range search.
 *
 * @param <K> The Key in a Key + Value pair
 * @param <V> The Value in a Key + Value pair
 */
public record SearchResult<K, V>(Tuple<K, V> tuple, double distance) implements Comparable<SearchResult<K, V>> {

    /** The key of this result's Tuple. */
    public K key() {
        return tuple.key();
    }

    /** The value of this result's Tuple. */
    public V value() {
        return tuple.value();
    }

    /** The id of this result's Tuple. */
    public TimeId id() {
        return tuple.id();
    }

    /**
     * Sort by distance. This is required for the PriorityQueue used to collect the Results always
     * has the Result with the k-th largest distance on top. This means the threshold for improving
     * the k-nearest neighbor result is readily accessible.
     */
    @Override
    public int compareTo(SearchResult<K, V> other) {
        return Double.compare(other.distance, this.distance);
    }
}
