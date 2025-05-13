package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Search is an executable process that iterates through a DurableMetricTree collecting the
 * Key+Value Pairs that are "close" to the search key.
 * <p>
 * Searches can be "k-nearest neighbors" or "all within range" searches.
 *
 * @param <K> The "Key" class is used to measure distance between two objects
 * @param <V> The "Value" class
 */
class Search<K, V> {

    static final Logger LOGGER = LoggerFactory.getLogger(Search.class);

    /*
     * Implementation note: the original version of this class was written for the original
     * MetricTree data structure.  Because that was from so long ago this class retains a
     * "doofus-driven design" feature where a single search object performs both RANGE searches and
     * KNN searches.  This isn't the worse double-booking of responsibilities, so I'm leaving it as
     * is because...at least the code works even if the design is iffy at best.
     */

    private enum SearchType {
        K_NEAREST_NEIGHBORS,
        RANGE
    }

    private final SearchType type;

    private final K searchKey;

    private final int maxNumResults; // only used for kNN searches

    private final double fixedRadius; // only used for range searches

    private final PriorityQueue<SearchResult<K, V>> resultsQueue;

    private final InternalTree<K, V> dataStore;

    private boolean isDone = false;

    private Search(SearchType type, K searchKey, InternalTree<K, V> dataStore, double limit) {
        requireNonNull(type);
        requireNonNull(searchKey);
        requireNonNull(dataStore);
        checkArgument(limit > 0);

        this.searchKey = searchKey;
        this.type = type;

        if (type == SearchType.K_NEAREST_NEIGHBORS) {
            this.maxNumResults = (int) limit;
            this.fixedRadius = Double.POSITIVE_INFINITY;
        } else {
            this.maxNumResults = Integer.MAX_VALUE;
            this.fixedRadius = limit;
        }

        this.resultsQueue = new PriorityQueue<>();
        this.dataStore = dataStore;
    }

    /**
     * Create a kNN search query.
     *
     * @param searchKey Search for this
     * @param k         The "k" in k-Nearest-Neighbors
     * @param dataStore The source of information about the DurableMetricTree
     */
    static <K, V> Search<K, V> knnSearch(K searchKey, int k, InternalTree<K, V> dataStore) {
        return new Search<>(SearchType.K_NEAREST_NEIGHBORS, searchKey, dataStore, k);
    }

    /**
     * Create a range query that returns all tuples within range
     *
     * @param searchKey Search for this
     * @param range     Include results within this distance
     * @param dataStore The source of information about the DurableMetricTree
     */
    static <K, V> Search<K, V> rangeSearch(K searchKey, double range, InternalTree<K, V> dataStore) {
        return new Search<>(SearchType.RANGE, searchKey, dataStore, range);
    }

    /*
     * Note: This search process cannot be written as a recursive search. Searching recursively can
     * produce a StackoverflowError when the underlying tree is deeper than the JVM's internal stack
     */
    synchronized void executeQuery() {

        if (isDone) {
            LOGGER.atWarn()
                    .setMessage("Attempting to (re)execute a completed search")
                    .log();
            return;
        }

        NodeHeader<K> rootNode = dataStore.rootNode();
        if (isNull(rootNode)) {
            isDone = true;
            return;
        }

        /*
         * As we descend the tree towards a leaf node we'll push "nodes that need to be explored"
         * onto the stack. We want to push "bad nodes" onto the stack early and "good nodes" onto
         * the stack late. This way the "good nodes" get popped off the stack earlier and a "good
         * solution" is found earlier. Thus, we'll correctly skip more data and reduce the number
         *  of operations needed to find the solution.
         */
        Deque<NodeHeader<K>> stackOfNodesToSearch = new ArrayDeque<>();
        stackOfNodesToSearch.push(rootNode);

        while (!stackOfNodesToSearch.isEmpty()) {

            NodeHeader<K> currentNode = stackOfNodesToSearch.pop();

            // Ignore this node (and all its subtrees). It cannot improve the current result
            if (!this.overlapsWith(currentNode)) {
                continue;
            }

            if (currentNode.isLeafNode()) {
                DataPage<K, V> leafEntries = dataStore.dataPageAt(currentNode.id());
                ingestLeafTuples(leafEntries.tuples());
            } else {

                List<NodeHeader<K>> childNodes = dataStore.nodesBelow(currentNode.id());

                /*
                 * IMPORTANT: Add the nodes from "worst" to "best".  This way we'll reduce total
                 * work done because we'll be more likely to skip spheres that are "far away".
                 */
                childNodes.stream()
                        .sorted(sortByDistanceToKey(searchKey))
                        .forEach(nodeHeader -> stackOfNodesToSearch.push(nodeHeader));
            }
        }

        isDone = true;
        // @todo -- Make this "set" the results field...
    }

    /* Update the "working solution" with these tuples. */
    private void ingestLeafTuples(Set<Tuple<K, V>> tuples) {

        for (Tuple<K, V> tuple : tuples) {

            SearchResult<K, V> r = new SearchResult<>(tuple, distanceBtw(searchKey, tuple.key()));

            if (r.distance() <= this.radius()) {
                this.resultsQueue.offer(r);
            }
        }

        // enforce the "k" in kNN search, while too big - remove the worst result
        while (resultsQueue.size() > this.maxNumResults) {
            resultsQueue.poll();
        }
    }

    /** @return True when the "query sphere" and this node's "sphere" overlap. */
    private boolean overlapsWith(NodeHeader<K> node) {

        double distance = distanceBtw(node.center(), this.searchKey);
        double overlap = node.radius() + this.radius() - distance;

        return overlap >= 0;
    }

    /**
     * @return The "inclusion radius" based on the type of query being executed and the quality of
     *     the current results (so we can avoid processing spheres that cannot contain better
     *     results)
     */
    private double radius() {

        if (type == SearchType.K_NEAREST_NEIGHBORS) {
            if (resultsQueue.size() < maxNumResults) {
                // radius is still large because we haven't found "k" results yet
                return Double.POSITIVE_INFINITY;
            } else {
                return resultsQueue.peek().distance(); // must beat this to improve
            }
        } else if (type == SearchType.RANGE) {
            return this.fixedRadius; // includes everything within this radius
        } else {
            throw new AssertionError("Should never get here");
        }
    }

    SearchResults<K, V> results() {
        checkState(isDone, "Search was not executed");

        return new SearchResults<>(searchKey, resultsQueue);
    }

    private double distanceBtw(K one, K two) {
        return dataStore.config().distMetric().distanceBtw(one, two);
    }

    private Comparator<NodeHeader<K>> sortByDistanceToKey(K key) {

        return (node1, node2) -> Double.compare(distanceBtw(key, node2.center()), distanceBtw(key, node1.center()));
    }
}
