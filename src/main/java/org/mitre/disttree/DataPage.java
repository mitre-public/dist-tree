package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * A DataPage contains a "Set of Tuples" that are all assigned to the same leaf node of a
 * DistanceTree. A DataPage is analogous to "page" or "block" of data from a traditional database
 * backed by a B-Tree.
 * <p>
 * Responding to DistanceTree queries requires getting ALL, or NONE, of the tuples within a
 * particular leaf node. Additionally, the I/O system that stores tuples will naturally want to
 * "load and cache" the data in a structure like this.
 * <p>
 * A Radius and "center point" is intentionally not stored as part of a DataPage because we want the
 * center and radius to be maintained in a separate object (i.e., the NodeHeader) that can be more
 * aggressively cached because it is smaller and uses fewer bytes.
 *
 * @param id     A unique ID that identifies this particular page of data.
 * @param tuples The tuples themselves
 * @param <K>    The Key class
 * @param <V>    The Value class
 */
public record DataPage<K, V>(TimeId id, Set<Tuple<K, V>> tuples) {

    /** Create a new DataPage with this TimeId and these Tuples. */
    public static <K, V> DataPage<K, V> asDataPage(TimeId id, Collection<Tuple<K, V>> tuples) {
        return new DataPage<>(id, new TreeSet<>(tuples));
    }

    public List<K> keyList() {
        // Cannot be a Set because keys can be repeated
        return tuples.stream().map(e -> e.key()).toList();
    }

    public Set<TimeId> idSet() {
        return tuples.stream().map(e -> e.id()).collect(toSet());
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    /** Shorthand for tuples().size(). */
    public int size() {
        return tuples.size();
    }

    /** Combine two DataPage that have the same id. */
    public static <K, V> DataPage<K, V> merge(DataPage<K, V> a, DataPage<K, V> b) {
        requireNonNull(a);
        requireNonNull(b);
        checkArgument(a.id.equals(b.id));

        TreeSet<Tuple<K, V>> allTuples = new TreeSet<>();
        allTuples.addAll(a.tuples);
        allTuples.addAll(b.tuples);

        return new DataPage<>(a.id, allTuples);
    }
}
