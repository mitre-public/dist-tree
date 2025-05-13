package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Math.log;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.mitre.caasd.commons.ids.TimeId.newId;
import static org.mitre.disttree.NodeHeader.newInnerNodeHeader;
import static org.mitre.disttree.NodeHeader.newLeafNodeHeader;
import static org.mitre.disttree.Ops.TreeOperation;
import static org.mitre.disttree.TupleAssignment.assign;
import static org.mitre.disttree.VerifyingDistanceMetric.verifyDistances;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.mitre.caasd.commons.ids.TimeId;
import org.mitre.disttree.Splitter.SplitResult;
import org.mitre.disttree.TreeConfig.RepackingMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TransactionMaker knows how DistanceTrees must change to ingest more data.
 * <p>
 * A TransactionMaker converts a batch of {@code new Tuple<K,V>} data to a suite of updates
 * to NodeHeaders and DataPages -- i.e., the TreeTransaction
 */
class TransactionMaker<K, V> {

    static final Logger LOGGER = LoggerFactory.getLogger(TransactionMaker.class);

    private final Batch<K, V> batch;

    // How many "old DataPages" should this transaction refresh?
    private final RepackingMode repackingMode;

    private final TreeDiffTracker<K, V> treeDiff;

    private final DistanceMetric<K> distMetric;

    // If we ever change up the Splitting strategy this field will need to be injected
    private final Splitter<K, V> splitter;

    /**
     * Create a TransactionMaker that knows how to interact with NodeHeaders and
     *
     * @param tree  The Tree this TransactionMaker will mutate
     * @param batch The data being added to the Tree
     */
    TransactionMaker(InternalTree<K, V> tree, Batch<K, V> batch) {
        requireNonNull(tree);
        // commenting this out to support "repack transactions" ... might be a better way
        //        requireNonNull(batch);
        //        checkArgument(batch.size() > 0, "Batch cannot be empty");
        this.batch = batch;
        this.repackingMode = tree.config().repackingMode;
        this.treeDiff = new TreeDiffTracker<>(tree);
        this.distMetric = verifyDistances(tree.config().distMetric());
        this.splitter = new Splitter<>(distMetric);
    }

    TreeTransaction<K, V> computeTransaction() {
        checkState(!treeDiff.wasBuilt, "Can use exactly once");

        if (isNull(batch)) {
            return repackTree();
        }

        // Let the TreeDiff know which tuple IDs are new in this transaction
        // (so it know when to use CREATE instead of UPDATE)
        treeDiff.setIdsOfNewTuples(batch.entryIds());

        OpList<K, V> opList = treeDiff.basicOpsFor(batch);
        return asTreeTransaction(opList);
    }

    /**
     * Convert a list of TreeOperations into an "atomic" transaction that will mutate the
     * DistanceTree from one valid state to another containing "what needs to be written".
     */
    private TreeTransaction<K, V> asTreeTransaction(OpList<K, V> opList) {

        if (opList.isSeedingTreeForFirstTime()) {
            return initialTransactionForRootNode(opList);
        }

        List<NodeHeader<K>> resultingHeaders = opList.resultingHeaders();
        List<TupleAssignment<K, V>> tupleAssignments = opList.tupleAssignments();

        treeDiff.putAllNodes(resultingHeaders);
        treeDiff.putAllTuples(tupleAssignments);

        splitNodesQuickly();

        repack(findBestRepacks());
        rebuildOldestLeaves(numLeavesToRebuild());

        return treeDiff.asTransaction();
    }

    /**
     * Repack the TreeTransaction we are in the process of building.
     * <p>
     * Repacking a transaction is shorthand for: "Remove all Tuples from a DataPage, then reinsert
     * those tuples as if they were newly inserted into the tree, finally, trim any unused nodes"
     */
    private void repack(Set<TimeId> leavesToRepack) {
        /*
         * IMPORTANT! It is WRONG to assume repacked DataPage must have at least 1 entry after the
         * repack. It can be the case that ZERO tuples are assigned to a DataPage after repacking.
         * In this case we must delete the DataPage and any references to it.
         */
        LOGGER.atTrace()
                .setMessage("Repacking {} leaves")
                .addArgument(leavesToRepack.size())
                .log();

        leavesToRepack.forEach(id -> LOGGER.atTrace().log("Repacking: {} ", id));

        // Get a list of all the tuples across all the leaves that need to be repacked
        List<Tuple<K, V>> tuplesToRepack = leavesToRepack.stream()
                .map(id -> treeDiff.curDataPageAt(id))
                .flatMap(page -> page.tuples().stream())
                .toList();

        // Set the radius and size of each Node we are repacking to zero.
        // This allows the radius of repacked Pages to shrink! -- thus speeding up Searches
        // This also ensures the resulting NodeHeaders have the correct tupleCounts
        leavesToRepack.stream()
                .map(id -> treeDiff.curNodeAt(id))
                .map(leaf -> leaf.zeroRadiusZeroTupleCopy())
                .forEach(smallLeaf -> treeDiff.putNode(smallLeaf));

        // Generate the TreeOperation required to "reinsert" these tuples into the tree
        List<TreeOperation<K, V>> rawOps = tuplesToRepack.stream()
                .flatMap(entry -> treeDiff.basicOpsFor(entry).stream())
                .toList();

        OpList<K, V> opList = new OpList<>(rawOps);

        leavesToRepack.forEach(id -> treeDiff.deletePage(id));

        List<NodeHeader<K>> resultingHeaders = opList.resultingHeaders();
        List<TupleAssignment<K, V>> tupleAssignments = opList.tupleAssignments();

        treeDiff.putAllNodes(resultingHeaders);
        treeDiff.putAllTuples(tupleAssignments);

        splitNodesCarefully();

        // Handle case where repacking a DataPage leaves that DataPage COMPLETELY EMPTY!
        // When this occurs, the parent's NodeHeader must be updated to reflect leaf deletion

        // DataPages that were "pruned but grew back" should not be in the deleted set
        List<TimeId> regrownLeaves =
                resultingHeaders.stream().map(NodeHeader::id).toList();

        TreeSet<TimeId> deletedLeaves = new TreeSet<>(leavesToRepack);
        deletedLeaves.removeAll(regrownLeaves);

        for (TimeId deletedLeafId : deletedLeaves) {
            NodeHeader<K> deleteMe = treeDiff.curNodeAt(deletedLeafId);
            removeNodeFromTree(deleteMe);
        }
    }

    /** Compute how many "old leaf nodes" should be rebuilt when a batch is written to the tree. */
    private int numLeavesToRebuild() {

        return switch (repackingMode) {
            case NONE -> 0;
                // This is a reasonable default because we amortize the maintenance work
            case INCREMENTAL_LN -> (int) (log(treeDiff.numLeafNodes()) + 1);
        };
    }

    private void rebuildOldestLeaves(int n) {

        // @todo -- This could be improved by 1st finding the TimeIds of the n oldest leaves then calling
        // rebuild(TimeId) .. for each id
        for (int i = 0; i < n; i++) {
            rebuildOldestLeaf();
        }
    }

    private void rebuildOldestLeaf() {

        if (treeDiff.curRootNode().numChildren() < 3) {

            LOGGER.atTrace()
                    .setMessage("Skipping replacing oldest leaf because tree has less than 3 leafs")
                    .log();

            return;
        }

        TimeId oldestLeaf = treeDiff.oldestLeafNode();
        TimeId newLeafId = TimeId.newId();

        /*
         * IMPORTANT! It is WRONG to assume repacked DataPage must have at least 1 entry after the
         * repack. It can be the case that ZERO tuples are assigned to a DataPage after repacking.
         * In this case we must delete the DataPage and any references to it.
         */
        LOGGER.atTrace()
                .setMessage("Replacing oldest leaf {} with {}")
                .addArgument(oldestLeaf)
                .addArgument(newLeafId)
                .log();

        // Get a list of all the tuples across all the leaves that need to be repacked
        Set<Tuple<K, V>> tuplesToRepack = treeDiff.curDataPageAt(oldestLeaf).tuples();

        // Now manually REPLACE the oldest node, we want to retain its old center point key
        NodeHeader<K> oldestLeafHeader = treeDiff.curNodeAt(oldestLeaf);
        NodeHeader<K> parent = treeDiff.curNodeAt(oldestLeafHeader.parent());

        // Replicate this leaf, but with a new id
        NodeHeader<K> newLeaf = NodeHeader.newLeafNodeHeader(newLeafId, parent.id(), oldestLeafHeader.center(), 0, 0);

        NodeHeader<K> updatedParent = parent.replaceChild(oldestLeaf, newLeafId);

        treeDiff.deletePage(oldestLeaf);
        treeDiff.deleteNode(oldestLeaf);
        treeDiff.putNode(newLeaf);
        treeDiff.putNode(updatedParent);

        // Rebuilding the oldest DataPage removes that DataPage COMPLETELY
        // But! We retain the original sort key. It was selected wisely, don't throw it out.
        // We want to refresh the DataPage's TimeIds so we force delete it .
        // If it grows back ,it grows back with a new TimeId.

        // Generate the TreeOperation required to "reinsert" these tuples into the tree
        List<TreeOperation<K, V>> rawOps = tuplesToRepack.stream()
                .flatMap(entry -> treeDiff.basicOpsFor(entry).stream())
                .toList();

        OpList<K, V> opList = new OpList<>(rawOps);

        List<NodeHeader<K>> resultingHeaders = opList.resultingHeaders();
        List<TupleAssignment<K, V>> tupleAssignments = opList.tupleAssignments();

        treeDiff.putAllNodes(resultingHeaders);
        treeDiff.putAllTuples(tupleAssignments);

        // see if the newLeaf, whose initial size was 0, had its size incremented ...
        Optional<NodeHeader<K>> opt =
                resultingHeaders.stream().filter(node -> node.hasId(newLeafId)).findFirst();
        if (opt.isEmpty()) {
            removeNodeFromTree(newLeaf);
        }

        // Must come after removing any rebuilt leaf that goes unused!
        splitNodesCarefully();
    }

    TreeTransaction<K, V> repackTree() {
        int n = treeDiff.numLeafNodes();

        for (int i = 2; i < n; i++) {
            rebuildOldestLeaf();
        }

        return treeDiff.asTransaction();
    }

    /**
     * This is a recursive function, it will remove a node's parent node when necessary.
     *
     * @param deleteMe The node that is being removed (This is NOT a TimeId because sometimes we
     *                 just removed this node from the "headerUpdates" list and the node contains
     *                 changes that won't be reflected in a call to "tree.nodeAt(id)"
     */
    private void removeNodeFromTree(NodeHeader<K> deleteMe) {

        LOGGER.atTrace().log("Deleting Node {}", deleteMe.id());

        treeDiff.deleteNode(deleteMe.id());

        NodeHeader<K> parent = treeDiff.curNodeAt(deleteMe.parent());
        NodeHeader<K> smallerParent = parent.removeChild(deleteMe.id());
        // ^^^^ This is the mutation that gets lost if we just use TimeIds

        if (smallerParent.numChildren() == 0) {
            LOGGER.atTrace().log("Must also delete the parent of {}", deleteMe.id());
            removeNodeFromTree(smallerParent);
        } else {
            treeDiff.putNode(smallerParent);
        }
    }

    /** @return A TreeTransaction that will build one inner node, one leaf node, and one DataPage. */
    private TreeTransaction<K, V> initialTransactionForRootNode(OpList<K, V> ops) {

        // Isolate all the "wanna-bes root Tuples".
        List<Tuple<K, V>> seedEntries = ops.extractSeedTuples();

        K center = seedEntries.get(0).key();

        double radius = distMetric.computeRadius(
                center, seedEntries.stream().map(e -> e.key()).toList());

        TimeId rootId = newId();
        TimeId leafId = newId();

        treeDiff.registerNewNode(rootId);
        treeDiff.registerNewNode(leafId);

        NodeHeader<K> rootNode = newInnerNodeHeader(rootId, null, center, radius, List.of(leafId));
        NodeHeader<K> leafNode = newLeafNodeHeader(leafId, rootId, center, radius, seedEntries.size());

        treeDiff.putNode(rootNode);
        treeDiff.putNode(leafNode);

        // Assign all "seed tuples" to the initial root node
        List<TupleAssignment<K, V>> assignments = seedEntries.stream()
                .map(tuple -> new TupleAssignment<>(tuple, leafId))
                .toList();
        assignments.forEach(ta -> treeDiff.putTupleAssignment(ta));

        splitNodesCarefully();

        // Should the initial transaction be repacked ???

        return treeDiff.asTransaction();
    }

    /** Used when leaf nodes will be immediately repacked. */
    private void splitNodesQuickly() {
        splitNodes(true);
    }

    private void splitNodesCarefully() {
        splitNodes(false);
    }

    /**
     * Before a TreeTransaction is finalized, search through the "working-transaction state" for any
     * overflowing nodes.  If found, alter the transaction by splitting the node and updating the
     * "working-transaction state data" accordingly.
     *
     * @param splitLeavesQuickly When true leaf splits will NOT use the distance metric to reassign
     *                           Tuples to Pages. We use this option when we know these leaves will
     *                           be repacked almost immediately so there is no need to execute this
     *                           computation because it will be immediately thrown out.
     */
    private void splitNodes(boolean splitLeavesQuickly) {

        // If we are going to create a Leaf Node that holds "too much data"
        //  ... we'll need to split that Leaf node into an inner node with 2 childNodes.
        while (treeDiff.hasSplittableHeader()) {

            NodeHeader<K> nodeToSplit = treeDiff.findOneSplittableNode();

            if (nodeToSplit.isRoot()) {
                // insert new root,
                pushDownRoot(nodeToSplit);
                // DO NOT split the old root, wait until it gets split as just a regular inner node
                continue;
            }

            if (nodeToSplit.isLeafNode()) {
                splitLeaf(nodeToSplit, splitLeavesQuickly);
            } else {
                splitInner(nodeToSplit);
            }
        }
    }

    /**
     * Push down the root node one layer, this prepares the "old root" to be split like a normal
     * inner node.
     *
     * @param curRoot The current root node
     */
    private void pushDownRoot(NodeHeader<K> curRoot) {
        checkArgument(curRoot.isRoot());

        // spawn a new root NodeHeader
        TimeId newRootId = newId();
        NodeHeader<K> newRoot =
                newInnerNodeHeader(newRootId, null, curRoot.center(), curRoot.radius(), List.of(curRoot.id()));

        treeDiff.registerNewNode(newRootId);

        // Tell the "oldRoot" is now has a parent node
        NodeHeader<K> updatedOldRoot = curRoot.withParent(newRootId);

        // Add the updates to the list for future committing
        treeDiff.putNode(newRoot);
        treeDiff.putNode(updatedOldRoot);

        LOGGER.atTrace()
                .setMessage("Adding new root at {}, pushing down {}")
                .addArgument(newRootId)
                .addArgument(curRoot.id())
                .log();
    }

    /**
     * An "over-sized leaf node" becomes two 2 leaf nodes at the same level in the tree
     *
     * @param nodeToSplit        The "over-sized" leaf node that will be split
     * @param splitLeavesQuickly True when "tuple assignment" should be quick and sloppy because the
     *                           new leaves will immediately be repacked (aka the computation to
     *                           perform perfect splits is wasted)
     */
    private void splitLeaf(NodeHeader<K> nodeToSplit, boolean splitLeavesQuickly) {

        checkState(!nodeToSplit.isRoot(), "Cannot split the root node");
        checkState(nodeToSplit.isLeafNode(), "Requires leaf node\n  " + nodeToSplit);

        // Find **ALL** the data that would have been at the "over-sized leaf node"
        DataPage<K, V> combined = treeDiff.curDataPageAt(nodeToSplit.id());

        // Purge the data at this leaf because all of it is being reassigned amongst the spits
        treeDiff.deletePage(nodeToSplit.id());

        // Split quickly when the resulting leaf nodes will be repacked (i.e. lost)
        // Split carefully when the resulting leaf assignments will be kept
        SplitResult<K, V> split =
                splitLeavesQuickly ? splitter.quickThrowAwaySplit(combined) : splitter.splitCarefully(combined);

        TimeId newLeafId = newId();
        treeDiff.registerNewNode(newLeafId);

        // These new Leaf nodes introduced new "KEY center" -- IF! We perform a repack operation it makes sense to start
        // with leaves near these Keys
        treeDiff.registerRepackSeed(nodeToSplit.id());
        treeDiff.registerRepackSeed(newLeafId);

        String how = splitLeavesQuickly ? "Quickly " : "Carefully ";

        LOGGER.atDebug()
                .setMessage(how + "Splitting leaf: {} into: {} and {} sizes {} and {}")
                .addArgument(nodeToSplit.id())
                .addArgument(nodeToSplit.id())
                .addArgument(newLeafId)
                .addArgument(split.left().tuples().size())
                .addArgument(split.right().tuples().size())
                .log();

        // Create the new leaf nodes ...

        // When we add a new leaf node we also need to update the parent node (about its new child)
        TimeId parentId = nodeToSplit.parent();

        NodeHeader<K> oldParentHeader = treeDiff.curNodeAt(nodeToSplit.parent());
        NodeHeader<K> newParentHeader = oldParentHeader.addChild(newLeafId);

        treeDiff.putNode(newParentHeader);

        NodeHeader<K> leftLeaf = stubToNode(nodeToSplit.id(), parentId, split.left());
        NodeHeader<K> rightLeaf = stubToNode(newLeafId, parentId, split.right());

        // Add the two leaf node headers
        treeDiff.putNode(leftLeaf);
        treeDiff.putNode(rightLeaf);

        //  Reinsert the entry data...
        var leftEntries = split.left().tuples();
        var rightEntries = split.right().tuples();
        leftEntries.forEach(tuple -> treeDiff.putTupleAssignment(assign(tuple, leftLeaf.id())));
        rightEntries.forEach(tuple -> treeDiff.putTupleAssignment(assign(tuple, rightLeaf.id())));
    }

    /**
     * Split an inner nodes with too many childNodes. This change yields 2 inner nodes at the same
     * level in the tree.  One node will be brand new, The other is the "smaller" version of
     * "nodeToSplit".
     *
     * @param nodeToSplit The "over-sized" leaf node that will be split
     */
    private void splitInner(NodeHeader<K> nodeToSplit) {
        checkState(!nodeToSplit.isRoot(), "Cannot split the root node");
        checkState(nodeToSplit.isInnerNode(), "Requires inner node\n " + nodeToSplit);

        // Find all the children that need to be reallocated btw the replacement and sibling
        List<NodeHeader<K>> children = nodeToSplit.childNodes().stream()
                .map(id -> treeDiff.curNodeAt(id))
                .toList();

        checkArgument(!children.contains(null), "Cannot have a null child");

        // This "FURTHEST" approach might be perfect fine...there could just be another bug breaking this assumption...

        //        // Find the node "furthest" from the node we must keep (e.g., nodeToSplit)
        //        // This is "electing" the center point of the new "sibling node"
        //        // This gives us the best chance to reduce the radius of the these two nodes
        //        NodeHeader<K> furthest = chooseFurthest(distMetric, children, nodeToSplit.center()).node();
        //        SphereCenters<K> centers = new SphereCenters<>(nodeToSplit.center(), furthest.center());

        List<K> centers =
                splitter.split(children.stream().map(NodeHeader::center).toList());

        // Divide up nodeToSplit's childNodes into two lists,
        SphereCenters<K> centers2 = new SphereCenters<>(centers.get(0), centers.get(1));
        NodeLists<K> childLists = centers2.divideChildren(children, distMetric);

        // Estimate the radius of each new inner node
        double radius1 = distMetric.estimateInnerNodeRadius(centers2.key1, childLists.list1);
        double radius2 = distMetric.estimateInnerNodeRadius(centers2.key2, childLists.list2);

        checkState(childLists.totalSize() == children.size(), "change in child count!");
        checkState(
                !childLists.hasEmptyList(), "list sizes: " + childLists.list1.size() + " " + childLists.list2.size());

        // Create a "smaller version" of nodeToSplit that has fewer children and a smaller radius
        NodeHeader<K> replacement = newInnerNodeHeader(
                nodeToSplit.id(),
                nodeToSplit.parent(),
                centers2.key1,
                radius1,
                childLists.list1.stream().map(node -> node.id()).toList());

        // Create a "sibling node" that has some of the children and a smaller radius
        NodeHeader<K> sibling = newInnerNodeHeader(
                newId(),
                nodeToSplit.parent(),
                centers2.key2,
                radius2,
                childLists.list2.stream().map(node -> node.id()).toList());

        treeDiff.registerNewNode(sibling.id());

        // Update the parent node so it "gets" this new sibling
        NodeHeader<K> existingParent = treeDiff.curNodeAt(nodeToSplit.parent());
        NodeHeader<K> updatedParent = existingParent.addChild(sibling.id());

        // The children of the sibling need to have their parent node updated...
        for (NodeHeader<K> node : childLists.list1) {
            // @todo -- these nodes MAY OR MAY NOT have been altered, this is "writing too much"
            // Some child nodes may not change parent node didn't change don't need to be sent (because they didn't
            // change!)
            treeDiff.putNode(node);
        }

        // The children of the sibling need to have their parent node updated...
        for (NodeHeader<K> node : childLists.list2) {
            NodeHeader<K> updated = node.withParent(sibling.id());
            treeDiff.putNode(updated);
        }

        // Add the two inner node headers
        treeDiff.putNode(replacement);
        treeDiff.putNode(sibling);
        treeDiff.putNode(updatedParent);

        LOGGER.atDebug()
                .setMessage("Splitting inner node at {} into: {} and {} with {} children and {} children")
                .addArgument(nodeToSplit.id())
                .addArgument(nodeToSplit.id())
                .addArgument(sibling.id())
                .addArgument(replacement.numChildren())
                .addArgument(sibling.numChildren())
                .log();
    }

    /** Determine which DataPages should be repacked in this transaction. */
    private Set<TimeId> findBestRepacks() {

        // Always repack the leaf nodes that were just built!
        // New leaf nodes ALWAYS introduce "space overlap".
        // New leaves introduce "new center points" that change the shape of the "Voronoi" polygons
        // Here we combat half of that "space overlap"
        return new TreeSet<>(treeDiff.repackSeeds());

        // Note: We no longer repack "the biggest sibling of each new leaf node"
        // This strategy can sometimes waste time by repeatedly repacking the same node
        // "Rebuilding the oldest leaf" will be more robust
    }

    private record SphereCenters<K>(K key1, K key2) {

        NodeLists<K> divideChildren(List<NodeHeader<K>> children, DistanceMetric<K> distMetric) {

            List<NodeHeader<K>> list1 = newArrayList();
            List<NodeHeader<K>> list2 = newArrayList();

            boolean tieBreaker = false;

            for (NodeHeader<K> child : children) {

                double dist1 = distMetric.distanceBtw(key1, child.center());
                double dist2 = distMetric.distanceBtw(key2, child.center());

                // Determine the best list for this child.  MUST ALTERNATE FOR TIES!!
                List<NodeHeader<K>> bestList;
                if (dist1 == dist2) {
                    bestList = tieBreaker ? list1 : list2;
                    tieBreaker = !tieBreaker; // alternate the tiebreaker
                } else {
                    bestList = (dist1 < dist2) ? list1 : list2;
                }

                bestList.add(child);
            }

            return new NodeLists<>(list1, list2);
        }
    }

    private record NodeLists<K>(List<NodeHeader<K>> list1, List<NodeHeader<K>> list2) {

        int totalSize() {
            return list1.size() + list2.size();
        }

        boolean hasEmptyList() {
            return list1.isEmpty() || list2.isEmpty();
        }
    }

    /** Convert a "Split Stub" to a NodeHeader, aka assign a Route to this stub. */
    private NodeHeader<K> stubToNode(TimeId newId, TimeId parentId, Splitter.Stub<K, V> stub) {
        return new NodeHeader<>(
                newId,
                parentId,
                stub.center(),
                stub.radius(),
                null,
                stub.tuples().size());
    }
}
