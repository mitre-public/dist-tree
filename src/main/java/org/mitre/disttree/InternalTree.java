package org.mitre.disttree;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Collections.emptyList;
import static java.util.Objects.*;
import static java.util.stream.Collectors.toSet;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.mitre.caasd.commons.ids.TimeId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.StatsAccumulator;

/**
 * InternalTree is intentionally not public because: (1) its API exposes too much internal detail
 * and (2) the methods are fragile because they return the entire tree's contents.
 * <p>
 * DistanceTree's API is designed to support robustly unit testing how the tree structure is
 * impacted by applying TreeTransactions. These "tree inspection methods" are NOT SUITABLE for
 * safely querying a large tree in a production environment (many OutOfMemoryExceptions would be
 * thrown).
 * <p>
 * To maximize performance, InternalTree only supports writing batches of data to the tree.
 * <p>
 * Implementation Note: We strongly considered combining DistanceTree and InternalTree into a single
 * class that has public methods and many other non-public methods that were clearly not suitable
 * for external users.  We (barely) rejected combining the classes because the code was clearer with
 * the stronger separation.
 *
 * @param <K>
 * @param <V>
 */
class InternalTree<K, V> {

    private final TreeConfig<K, V> config;

    /** Converts Keys and Values to and from byte[]. */
    private final SerdePair<K, V> serdePair;

    /** Handles I/O with raw byte[]. */
    private final DataStore binaryDataStore;

    InternalTree(TreeConfig<K, V> config) {

        requireNonNull(config);
        requireNonNull(config.serdePair());
        requireNonNull(config.distMetric());
        requireNonNull(config.dataStore);
        this.config = config;
        this.serdePair = config.serdePair();
        this.binaryDataStore = config.dataStore;
    }

    TreeConfig<K, V> config() {
        return this.config;
    }

    DataStore dataStore() {
        return this.binaryDataStore;
    }

    /**
     * @return The id of the last TreeTransaction that altered this tree. This id helps us ensure
     *     TreeTransactions are ALWAYS "applied to" The DistanceTree state they were "built from".
     *     It also tells us which TreeTransactions have been incorporated into the DistanceTree.
     *     <p>
     *     If you think of TreeTransactions as individual git commits then the lastTransactionId
     *     helps us detect "git conflicts" (e.g., when two TreeTransactions try to change the same
     *     tree)
     */
    TimeId lastTransactionId() {
        return binaryDataStore.lastTransactionId();
    }

    TimeId rootId() {
        return binaryDataStore.rootId();
    }

    NodeHeader<K> rootNode() {
        return nodeAt(rootId());
    }

    DataPage<K, V> dataPageAt(TimeId id) {
        requireNonNull(id);

        // pull the data from the binaryDataStore....deserialize and return
        DataPage<byte[], byte[]> rawEntries = binaryDataStore.dataPageAt(id);
        return isNull(rawEntries) ? null : serdePair.deserialize(rawEntries);
    }

    NodeHeader<K> nodeAt(TimeId id) {
        if (isNull(id)) {
            return null;
        }

        var rawHeader = binaryDataStore.nodeAt(id);

        return nonNull(rawHeader) ? serdePair.deserializeHeader(rawHeader) : null;
    }

    List<NodeHeader<K>> nodesBelow(TimeId nodeId) {

        NodeHeader<K> node = nodeAt(nodeId);
        if (node.isLeafNode()) {
            return emptyList();
        }

        List<TimeId> childIds = node.childNodes();

        return childIds.stream().map(id -> nodeAt(id)).filter(Objects::nonNull).toList();
    }

    /** @return Stats on the tree's size and shape. This scans the NodeHeader dataset once. */
    public TreeStats treeStats() {

        int tuples = 0;
        int leafNodes = 0;
        int innerNodes = 0;
        StatsAccumulator radiusStats = new StatsAccumulator();

        for (NodeHeader<K> node : allNodes()) {
            if (node.isLeafNode()) {

                radiusStats.add(node.radius());

                leafNodes++;
                tuples += node.numTuples();
            } else {
                innerNodes++;
            }
        }

        // When the tree has 1 leaf node sampleStandardDeviation() will fail, avoid this
        double sigma = (leafNodes != 1) ? radiusStats.sampleStandardDeviation() : 0;

        return new TreeStats(tuples, leafNodes, innerNodes, radiusStats.mean(), sigma);
    }

    /** Exists to launch inefficient, exhaustive, processes that verify the state of the tree. */
    @VisibleForTesting
    List<NodeHeader<K>> allNodes() {

        TreeMap<TimeId, NodeHeader<K>> uniqueNodes = new TreeMap<>();

        LinkedList<NodeHeader<K>> nodesToExplore = newLinkedList();
        nodesToExplore.add(nodeAt(rootId()));

        while (!nodesToExplore.isEmpty()) {
            NodeHeader<K> current = nodesToExplore.removeFirst();
            uniqueNodes.putIfAbsent(current.id(), current);

            nodesToExplore.addAll(nodesBelow(current.id()));
        }

        return newArrayList(uniqueNodes.values());
    }

    /** Not Suitable for large trees because you'll get OutOfMemoryExceptions. */
    @VisibleForTesting
    List<DataPage<K, V>> allDataPages() {
        return allNodes().stream()
                .filter(node -> node.isLeafNode())
                .map(node -> dataPageAt(node.id()))
                .filter(Objects::nonNull)
                .toList();
    }

    /** Not Suitable for large trees because you'll get OutOfMemoryExceptions. */
    @VisibleForTesting
    Set<Tuple<K, V>> tuples() {
        return allDataPages().stream()
                .flatMap(dataPage -> dataPage.tuples().stream())
                .collect(toSet());
    }

    @VisibleForTesting
    List<NodeHeader<K>> innerNodes() {
        return allNodes().stream().filter(node -> node.isInnerNode()).toList();
    }

    @VisibleForTesting
    List<NodeHeader<K>> leafNodes() {
        return allNodes().stream().filter(node -> node.isLeafNode()).toList();
    }
}
