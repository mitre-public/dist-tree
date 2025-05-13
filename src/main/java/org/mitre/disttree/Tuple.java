package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.mitre.caasd.commons.ids.TimeId.newId;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * A Tuple is a "unit of data" kept in the DistanceTree.  Tuples are comprised of the Key-Value Pair
 * we wish to store plus a unique ID.
 * <p>
 * Each Key-Value pair is decorated with a unique TimeId for sundry uses including: tracking
 * insertion time, randomly sampling data within the tree, providing a feature that will permit
 * evenly sharding the tree, and distinguishing Key-Value pairs with identical Key's.
 */
public record Tuple<K, V>(TimeId id, K key, V value) implements Comparable<Tuple<K, V>> {

    /** Draws a new TimeId and constructs a new Tuple. */
    public static <K, V> Tuple<K, V> newTuple(K key, V value) {
        return new Tuple<>(newId(), key, value);
    }

    /** Draws a new TimeId and constructs a new Tuple from a java.util.Map.Entry. */
    public static <K, V> Tuple<K, V> newTuple(Entry<K, V> entry) {
        return newTuple(entry.getKey(), entry.getValue());
    }

    public static <K> Tuple<K, Void> newSlimTuple(K key) {
        return new Tuple<>(newId(), key, null);
    }

    /** Zip n keys and n values together to form n Tuples. */
    public static <K, V> List<Tuple<K, V>> zipNewTuples(List<K> keys, List<V> values) {
        requireNonNull(keys);
        requireNonNull(values);
        checkArgument(keys.size() == values.size(), "Number keys must equal number of values");

        return IntStream.range(0, keys.size())
                .mapToObj(i -> newTuple(keys.get(i), values.get(i)))
                .toList();
    }

    @Override
    public int compareTo(Tuple<K, V> o) {
        return id.compareTo(o.id);
    }
}
