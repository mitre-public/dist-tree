package org.mitre.disttree;

import static java.util.Collections.*;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * SearchResults contain the results from a K-nearest Neighbors Search or a Range Search.  The
 * results are always sorted by distance to search Key.
 *
 * @param <K> The Key's making up the "Metric Space", we can measure the distance btw two Keys
 * @param <V> The Value type
 */
public class SearchResults<K, V> {

    /** The key provided at search time. */
    private final K searchKey;

    /** All result found during the Search operation. */
    private final ArrayList<SearchResult<K, V>> results;

    SearchResults(K searchKey, Collection<SearchResult<K, V>> c) {
        requireNonNull(searchKey);
        this.searchKey = searchKey;
        this.results = new ArrayList<>(c);
        results.sort(reverseOrder());
    }

    /** @return The Key upon which the search was based. */
    public K searchKey() {
        return searchKey;
    }

    /** @return True, when there is no data to report. */
    public boolean isEmpty() {
        return results.isEmpty();
    }

    /** @return The number of elements in the results set. */
    public int size() {
        return results.size();
    }

    /** Get all the result in a list sorted by distance (element 0 = "nearest neighbor"). */
    public List<SearchResult<K, V>> results() {
        return results;
    }

    /** Equivalent to {@code this.results().stream()}. */
    public Stream<SearchResult<K, V>> stream() {
        return results.stream();
    }

    /** Cherry-pick a single result from the sorted list of results (0 = closest Tuple). */
    public SearchResult<K, V> result(int i) {
        return results.get(i);
    }

    /** Get just the Tuples from the search results, list sorted by distance. */
    public List<Tuple<K, V>> tuples() {
        return results.stream().map(sr -> sr.tuple()).toList();
    }

    /** Get just the Keys from the search results, list sorted by distance. */
    public List<K> keys() {
        return results.stream().map(sr -> sr.key()).toList();
    }

    /** Get just the Values from the search results, list sorted by distance. */
    public List<V> values() {
        return results.stream().map(sr -> sr.value()).toList();
    }

    /** Get just the TimeIds from the search results, list sorted by distance. */
    public List<TimeId> ids() {
        return results.stream().map(sr -> sr.id()).toList();
    }

    /** Get just the distances from the search results, list sorted by distance. */
    public List<Double> distances() {
        return results.stream().map(sr -> sr.distance()).toList();
    }
}
