package org.mitre.disttree;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toCollection;
import static org.mitre.caasd.commons.ids.TimeId.newId;
import static org.mitre.disttree.Tuple.zipNewTuples;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.mitre.caasd.commons.ids.TimeId;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

/**
 * A Batch is collection of Tuples that need to be added to a DistanceTree via I/O operations.
 * <p>
 * A Batch of data will become to a single "TreeTransaction" that adds all these tuples at once.
 *
 * @param <K> The Keys of the Tuples
 * @param <V> The Values of the Tuples
 */
public class Batch<K, V> {

    /** The tuples waiting to be written to durable storage. */
    private final List<Tuple<K, V>> tuples;

    private final TimeId id;

    public Batch(List<Tuple<K, V>> tuples) {
        this.tuples = unmodifiableList(tuples);
        this.id = newId();
    }

    public int size() {
        return tuples.size();
    }

    public List<Tuple<K, V>> tuples() {
        return this.tuples;
    }

    public TimeId id() {
        return id;
    }

    public Instant creationTime() {
        return id.time();
    }

    public Set<TimeId> entryIds() {
        // Getting these ids up front helps distinguish "create entry ops" from "move entry ops"
        return tuples.stream().map(tuple -> tuple.id()).collect(toCollection(TreeSet::new));
    }

    /**
     * A BatchAccumulator holds incoming Tuples that will eventually be written to a DataStore.
     * Achieving efficient IO requires "bulk writes", this is where "add one tuples" becomes "add
     * many tuples".
     *
     * @param <K>
     * @param <V>
     */
    static class BatchAccumulator<K, V> {

        /** Tuples queuing up to be written to durable storage. */
        private final ArrayDeque<Tuple<K, V>> queue;

        public BatchAccumulator() {
            this.queue = new ArrayDeque<>();
        }

        public void addToBatch(Tuple<K, V> tuple) {
            queue.add(tuple);
        }

        public int currentBatchSize() {
            return queue.size();
        }

        /**
         * Drain all queued Tuples into a Batch<K,V> for committing to Tree via IO operations.
         */
        public Batch<K, V> drainToBatch() {

            synchronized (queue) {
                var allData = new ArrayList<>(queue);
                queue.clear();

                return new Batch<>(allData);
            }
        }
    }

    /** Convert a standard java.util.Map into a series of batches that can be added to a DistanceTree. */
    public static <K, V> List<Batch<K, V>> batchify(Map<K, V> data, int batchSize) {

        Iterator<Entry<K, V>> entryIterator = data.entrySet().iterator();
        Iterator<Tuple<K, V>> tupleIter = Iterators.transform(entryIterator, entry -> Tuple.newTuple(entry));

        return makeBatches(tupleIter, batchSize);
    }

    /** Convert a Collection of Tuples into multiple batches. */
    public static <K, V> List<Batch<K, V>> batchify(Collection<Tuple<K, V>> data, int batchSize) {
        return makeBatches(data.iterator(), batchSize);
    }

    /** Convert a Collection of Tuples into multiple batches. */
    public static <K, V> List<Batch<K, V>> batchify(List<K> keys, List<V> values, int batchSize) {
        List<Tuple<K, V>> tuples = zipNewTuples(keys, values);

        return batchify(tuples, batchSize);
    }

    /**
     * Convert a Collection of Keys into multiple Batches. The Batches, and their component Tuples,
     * do not use the values portion of the Tuples.
     */
    public static <K> List<Batch<K, Void>> batchifyKeys(Collection<K> keys, int batchSize) {
        List<Tuple<K, Void>> tuples =
                keys.stream().map(k -> Tuple.newSlimTuple(k)).toList();

        return batchify(tuples, batchSize);
    }

    /** "Walk" an Iterator of Tuples and produces batches of a fixed size. */
    private static <K, V> ArrayList<Batch<K, V>> makeBatches(Iterator<Tuple<K, V>> tupleIter, int batchSize) {

        // Use guava to partition the iterator contents
        UnmodifiableIterator<List<Tuple<K, V>>> parts = Iterators.partition(tupleIter, batchSize);
        ArrayList<Batch<K, V>> batches = new ArrayList<>();

        while (parts.hasNext()) {
            List<Tuple<K, V>> batchOfData = parts.next();
            Batch<K, V> batch = new Batch<>(batchOfData);
            batches.add(batch);
        }

        return batches;
    }
}
