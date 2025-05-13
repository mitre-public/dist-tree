package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Collections.emptyList;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.mitre.disttree.DataPage.merge;
import static org.mitre.disttree.DistBtw.chooseClosest;
import static org.mitre.disttree.DistBtw.measureDistBtw;
import static org.mitre.disttree.Misc.last;

import java.util.*;
import java.util.stream.Collectors;

import org.mitre.caasd.commons.ids.TimeId;
import org.mitre.disttree.Ops.TupleOp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TreeDiffTracker helps us mutate a DistanceTree using bug-free TreeTransactions.
 * <p>
 * TreeDiffTracker holds a fixed internal view of a DistanceTree as numerous updates to NodeHeaders
 * and TupleAssignment are made. Eventually, all tree mutations are collected and exported as a
 * single TreeTransaction. The transaction can be applied to a DistanceTree as a "ACID bulk update".
 * <p>
 * TreeDiffTracker's approach is similar to git. A git project has a "fixed remote state", a
 * "changing local state", and uses "commit diffs" to perform "bulk updates" to the remote state.
 * DistanceTrees are updated the same way. The TreeDiffTracker holds the "fixed initial state", gives
 * us a view of the "changing local state" and it produces the "commit diff" (i.e. the
 * TreeTransaction) we'll use to update the DistanceTree.
 * <p>
 * The TreeDiffTracker prevents conflating ongoing "in-flux" tree mutations with the DistanceTree's
 * original state.
 * <p>
 * A TreeDiffTracker does not implement the "normal" Builder pattern, but it is a Builder in
 * spirit.
 */
class TreeDiffTracker<K, V> {

    static final Logger LOGGER = LoggerFactory.getLogger(TreeDiffTracker.class);

    /** This is the tree we want to edit using a TreeTransaction. */
    final InternalTree<K, V> tree;

    /**
     * A "hash" of the DistanceTree's initial state. This transaction is invalid if this changes.
     * (i.e., concurrent modifications are not allowed)
     */
    private final TimeId lastTransactionId;

    /** NodeHeaders that will be updated during the transaction. */
    private final Map<TimeId, NodeHeader<K>> nodeUpdates;

    /** All the tuples that will be touched (i.e. created or updated) during the transaction. */
    public final Map<TimeId, TupleAssignment<K, V>> tupleAssignments;

    /** The Ids of DataPages that are deleted during the transaction. */
    private final Set<TimeId> deletedPages;

    /** The Ids of NodeHeaders that are deleted during the transaction. */
    private final Set<TimeId> deletedNodes;

    /**
     * The TimeId of any NodeHeader that get CREATED during this transaction. We keep track of these
     * ids so that when we build the resulting TreeTransaction we can distinguish "CREATE NODE" and
     * "UPDATE NODE" operations.
     */
    final Set<TimeId> idsOfNewNodes;

    /**
     * The TimeIds of any tuple that will be created by the output transaction. The set allows us to
     * distinguish "CREATE TUPLE" IO ops from "MOVE TUPLE" IO ops.  Additionally, the "set of
     * tuples" is known before we begin making a transaction so we can get this data up front.
     */
    final Set<TimeId> idsOfNewTuples;

    /**
     * When a leaf node is split we MAY want to immediately repack leaves that are "close" to these
     * leaves. Consequently, we keep track of which nodes got leafNodes got split -- so we know
     * where to find leaves that could benefit from a repack.
     */
    final Set<TimeId> idsOfRepackSeeds;

    boolean wasBuilt;

    /** Create a TransactionBuilder whose resulting TreeTransaction will alter this tree. */
    TreeDiffTracker(InternalTree<K, V> tree) {
        requireNonNull(tree);
        this.tree = tree;
        this.lastTransactionId = tree.lastTransactionId();
        this.nodeUpdates = new TreeMap<>();

        this.tupleAssignments = new TreeMap<>();

        this.deletedPages = new TreeSet<>();
        this.deletedNodes = new TreeSet<>();

        this.idsOfNewNodes = new TreeSet<>();
        this.idsOfNewTuples = new TreeSet<>();
        this.idsOfRepackSeeds = new TreeSet<>();

        wasBuilt = false;
    }

    void setIdsOfNewTuples(Set<TimeId> idsOfNewTuples) {
        this.idsOfNewTuples.addAll(idsOfNewTuples);
    }

    /**
     * Determines if we use a CREATE or UPDATE operations to write a NodeHeader data (basically,
     * anytime we invoke TimeId.newId() we should use a CREATE op).
     */
    void registerNewNode(TimeId idOfNewNode) {
        this.idsOfNewNodes.add(idOfNewNode);
    }

    /** Remember when a node is split, this is a good hint for starting a repack operation. */
    void registerRepackSeed(TimeId pageId) {
        this.idsOfRepackSeeds.add(pageId);
    }

    Set<TimeId> repackSeeds() {
        return idsOfRepackSeeds;
    }

    void putNode(NodeHeader<K> node) {
        nodeUpdates.put(node.id(), node);
    }

    void putAllNodes(Collection<NodeHeader<K>> nodes) {
        nodes.forEach(node -> putNode(node));
    }

    void deleteNode(TimeId id) {
        deletedNodes.add(id);
        nodeUpdates.remove(id);
    }

    void putTupleAssignment(TupleAssignment<K, V> ta) {
        tupleAssignments.put(ta.tuple().id(), ta);
    }

    void putAllTuples(Collection<TupleAssignment<K, V>> assignments) {
        assignments.forEach(ta -> putTupleAssignment(ta));
    }

    private List<TupleAssignment<K, V>> createdTuples() {
        return tupleAssignments.values().stream()
                .filter(ta -> idsOfNewTuples.contains(ta.tupleId()))
                .toList();
    }

    private List<TupleAssignment<K, V>> updatedTuples() {
        return tupleAssignments.values().stream()
                .filter(ta -> !idsOfNewTuples.contains(ta.tupleId()))
                .toList();
    }

    void deletePage(TimeId id) {
        deletedPages.add(id);
    }

    TreeTransaction<K, V> asTransaction() {

        checkState(!wasBuilt, "The TreeTransaction was already built and returned");

        wasBuilt = true;

        List<NodeHeader<K>> createdNodes = new ArrayList<>();
        List<NodeHeader<K>> updatedNodes = new ArrayList<>();
        nodeUpdates.values().forEach(node -> {
            if (idsOfNewNodes.contains(node.id())) {
                createdNodes.add(node);
            } else {
                updatedNodes.add(node);
            }
        });

        return new TreeTransaction<>(
                lastTransactionId,
                createdNodes,
                updatedNodes,
                createdTuples(),
                updatedTuples(),
                deletedPages,
                deletedNodes);
    }

    /**
     * Find the current version of a particular NodeHeader.  The node returned here will reflect any
     * changes that were submitted via the "putNode" methods.
     *
     * @param id The id of a NodeHeader that needs to be altered
     *
     * @return The "most up-to-date" edition of the requested NodeHeader
     */
    NodeHeader<K> curNodeAt(TimeId id) {
        requireNonNull(id);

        Optional<NodeHeader<K>> optHeader =
                nodeUpdates.values().stream().filter(header -> header.hasId(id)).findFirst();

        return optHeader.isPresent() ? optHeader.get() : tree.nodeAt(id);
    }

    /**
     * Builds a view of the current version of a particular DataPage. The returned page reflects
     * pre-existing DistanceTree state AND the impact of any changes submitted via the
     * "putTupleAssignment" method.
     *
     * @param id The id of a DataPage being retrieved
     *
     * @return The "most up-to-date" edition of the requested DataPage
     */
    DataPage<K, V> curDataPageAt(TimeId id) {
        requireNonNull(id);

        Set<Tuple<K, V>> tuplesInThisPage = tupleAssignments.values().stream()
                .filter(ta -> ta.hasPageId(id))
                .map(ta -> ta.tuple())
                .collect(Collectors.toSet());

        DataPage<K, V> page = new DataPage<>(id, tuplesInThisPage); // may be empty

        if (deletedPages.contains(id)) {
            // This leaf was deleted earlier in this transaction (probably during a split),
            // Therefore, we should ignore any preexisting data in the distanceTree
            return page;
        } else {
            // This leaf was NOT deleted,
            // Therefore, we should combine the "new tuples" with preexisting data in the distance tree
            DataPage<K, V> priors = tree.dataPageAt(id);
            return isNull(priors) ? page : merge(page, priors);
        }
    }

    /** @return A NodeHeader where "isSplittable" is true */
    NodeHeader<K> findOneSplittableNode() {

        int maxChildren = tree.config().branchingFactor();
        int maxTuples = tree.config().maxTuplesPerPage();

        NodeHeader<K> bigNode = nodeUpdates.values().stream()
                .filter(header -> header.isSplittable(maxChildren, maxTuples))
                .findFirst()
                .orElseThrow(AssertionError::new); // it's a logic error to call when there is no splittable node

        LOGGER.atTrace()
                .setMessage("Splitting {}: {} because it has {} {}")
                .addArgument(bigNode.isLeafNode() ? "leaf" : "inner node")
                .addArgument(bigNode.id())
                .addArgument(bigNode.isLeafNode() ? bigNode.numTuples() : bigNode.numChildren())
                .addArgument(bigNode.isLeafNode() ? "tuples" : "child nodes")
                .log();

        return bigNode;
    }

    /** @return True if the "working set" of NodeHeaders contains a NodeHeader that isSplittable. */
    boolean hasSplittableHeader() {

        int branchingFactor = tree.config().branchingFactor();
        int maxTuplesPerPage = tree.config().maxTuplesPerPage();

        return nodeUpdates.values().stream().anyMatch(node -> node.isSplittable(branchingFactor, maxTuplesPerPage));
    }

    /**
     * Deduces a reducible set of TreeOperations needed to insert a batch of tuples into this
     * DistanceTree. Naively performing these operations would cause a CASCADE of I/O operations to the
     * underlying DataStore that provides the "current tree state".
     *
     * @param batch A batch of Tuples being added to the tree
     *
     * @return A List of TreeOperation that represent making small changes to the tree (i.e.
     *     initializing a Root node, initializing the radius of a node, adding a Tuple to a leaf
     *     node, or increasing the "child count" of a leaf node)
     */
    OpList<K, V> basicOpsFor(Batch<K, V> batch) {

        List<Ops.TreeOperation<K, V>> basicOps = batch.tuples().stream()
                .flatMap(tuple -> basicOpsFor(tuple).stream())
                .toList();

        return new OpList<>(basicOps);
    }

    /**
     * Deduces the TreeOperations needed to insert a Tuple into this DistanceTree. Naively
     * performing this work causes a CASCADE of read operations to the underlying DataStore that is
     * providing the "current tree state". Consequently, efficient operation REQUIRES a caching
     * layer
     *
     * @param tuple An entry being added to the tree
     *
     * @return A List of TreeOperation that represent making small changes to the tree (i.e.
     *     initializing a Root node, initializing the radius of a node, adding a Tuple to a leaf
     *     node, or increasing the "child count" of a leaf node)
     */
    List<Ops.TreeOperation<K, V>> basicOpsFor(Tuple<K, V> tuple) {

        // the IO reads necessary to build this path better be cached!
        List<DistBtw<K>> path = pathToLeafFor(tuple.key());

        // the tree is completely empty!
        if (path.isEmpty()) {
            return List.of(new Ops.CreateRoot<>(tuple));
        }

        // Create a TreeOperation for each node that needs to have its distance expanded
        ArrayList<Ops.TreeOperation<K, V>> treeOps = path.stream()
                .filter(DistBtw::increasesRadius)
                .map(step -> (Ops.NodeOp<K, V>) step.asRadiusOp())
                .collect(toCollection((ArrayList::new))); // (not toList() for mutability)

        // At the leafNode: Add the Tuple to the Tree & increase the size of the leaf
        NodeHeader<K> leafNode = last(path).node();
        treeOps.add(new TupleOp<>(leafNode, tuple));
        treeOps.add(Ops.NodeOp.incrementTupleCount(leafNode));

        return treeOps;
    }

    /**
     * Compute the path to the leaf node whose center is closest to this key (this will be the best
     * place to find similar keys)
     * <p>
     * WARNING: The paths are computed on the unaltered
     *
     * @param key A Search Key
     *
     * @return The list of Distance measurements for each step in the path. The method return more
     *     than just the "best Route" to help us efficiently determine if we need to increase the
     *     distance of the node
     */
    List<DistBtw<K>> pathToLeafFor(K key) {

        NodeHeader<K> curNode = curRootNode();

        if (isNull(curNode)) {
            // the tree is empty...not even a root node!
            return emptyList();
        }

        List<DistBtw<K>> path = newArrayList();

        // handle root node
        path.add(measureDistBtw(tree.config().distMetric, curNode, key));

        List<NodeHeader<K>> nextLevelInTree = nodesBelow(curNode.id());

        // now append a RouteDist object for each child node
        while (!nextLevelInTree.isEmpty()) {
            DistBtw<K> bestChild = chooseClosest(tree.config().distMetric, nextLevelInTree, key);
            path.add(bestChild);

            nextLevelInTree = nodesBelow(bestChild.node().id());
        }

        return path;
    }

    List<NodeHeader<K>> nodesBelow(TimeId nodeId) {

        NodeHeader<K> node = curNodeAt(nodeId);
        if (node.isLeafNode()) {
            return emptyList();
        }

        List<TimeId> childIds = node.childNodes();

        return childIds.stream()
                .map(id -> curNodeAt(id))
                .filter(Objects::nonNull)
                .toList();
    }

    NodeHeader<K> curRootNode() {

        return nodeUpdates.values().stream()
                .filter(node -> node.isRoot())
                .findFirst()
                .orElse(tree.rootNode());
    }

    List<NodeHeader<K>> leafNodes() {

        TreeMap<TimeId, NodeHeader<K>> uniqueNodes = new TreeMap<>();

        LinkedList<NodeHeader<K>> nodesToExplore = newLinkedList();
        nodesToExplore.add(curRootNode());

        while (!nodesToExplore.isEmpty()) {
            NodeHeader<K> current = nodesToExplore.removeFirst();
            uniqueNodes.putIfAbsent(current.id(), current);

            nodesToExplore.addAll(nodesBelow(current.id()));
        }

        return newArrayList(uniqueNodes.values()).stream()
                .filter(node -> node.isLeafNode())
                .toList();
    }

    int numLeafNodes() {
        return leafNodes().size();
    }

    TimeId oldestLeafNode() {

        return leafNodes().stream()
                .map(node -> node.id())
                .min(Comparator.naturalOrder())
                .get();
    }
}
