package org.mitre.disttree;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mitre.disttree.Batch.batchify;
import static org.mitre.disttree.MiscTestUtils.randomBiModalLatLong;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.Tuple.newTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.mitre.caasd.commons.LatLong;
import org.mitre.caasd.commons.ids.TimeId;
import org.mitre.disttree.stores.InMemoryStore;

import com.google.common.collect.Sets;

public class SharedTestUtils {

    public static List<Tuple<LatLong, String>> createTestData(int n) {

        return IntStream.range(0, n)
                .mapToObj(i -> newTuple(randomLatLong(), Integer.toString(i)))
                .toList();
    }

    public static Map<LatLong, String> createTestMap(int n) {

        return IntStream.range(0, n).mapToObj(i -> Integer.toString(i)).collect(toMap(i -> randomLatLong(), i -> i));
    }

    public static List<Tuple<LatLong, String>> createBiModalTestData(int n) {

        return IntStream.range(0, n)
                .mapToObj(i -> newTuple(randomBiModalLatLong(), Integer.toString(i)))
                .toList();
    }

    public static List<Tuple<LatLong, String>> createTestDataWithNullValues(int n) {

        List<Tuple<LatLong, String>> data = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            data.add(newTuple(randomLatLong(), null));
        }
        return data;
    }

    static void addTestDataToTree(InternalTree<LatLong, String> privateTree, List<Tuple<LatLong, String>> testData) {
        addTestDataToTree(new DistanceTree<>(privateTree), testData);
    }

    static void addTestDataToTree(DistanceTree<LatLong, String> tree, List<Tuple<LatLong, String>> testData) {

        List<Batch<LatLong, String>> batches = batchify(testData, 50);
        tree.addBatches(batches);
    }

    /**
     * Rigorously verify a tree (paraphrasing here) "contains all this data" and "organizes it
     * correctly" (see implementation for details)
     *
     * @param testData A dataset that has been added to a Tree
     * @param tree     A view of the dataset
     */
    static void verifyTree(List<Tuple<LatLong, String>> testData, DistanceTree<LatLong, String> tree) {
        verifyTree(testData, tree.tree);
    }

    /**
     * Rigorously verify a tree (paraphrasing here) "contains all this data" and "organizes it
     * correctly" (see implementation for details)
     *
     * @param testData A dataset that has been added to a Tree
     * @param tree     A view of the dataset
     */
    static void verifyTree(List<Tuple<LatLong, String>> testData, InternalTree<LatLong, String> tree) {

        System.out.println("Verifying Tree");

        verifyRoot(tree);
        verifyNoOrphans(tree);

        verifyLeafNodesAndDataPageSizeMatches(tree);
        verifyLeafNodesObeyMaxTuplesPerNode(tree);
        verifyNoTupleInMultipleLeaves(tree);

        // basic tree constraints ...
        verifyAllDataContained(testData, tree); // start with this because it provides best info
        verifyEveryLeafNodeHasDataPage(tree);
        verifyNumChildrenSetCorrectly(tree);
        verifyTreeNodeRadii(tree);

        // inner node constraints ...
        verifyNoDataAtInnerNodes(tree);
        verifyInnerNodesHaveChildren(tree);
        verifyInnerNodeChildrenAreFound(tree);

        // leaf node constraints ...
        verifyAllDataInExactlyOneLeaf(testData, tree);
    }

    private static <K, V> void verifyRoot(InternalTree<K, V> tree) {
        TimeId rootId = tree.rootId();
        NodeHeader<K> root = tree.nodeAt(rootId);

        assertThat(rootId, not(nullValue()));
        assertThat(root.parent(), nullValue());

        System.out.println("  PASSED -- Root Node was found");
    }

    /** Verify The DataStore this Tree uses does not contain data that is "off the tree". */
    private static <K, V> void verifyNoOrphans(InternalTree<K, V> tree) {

        if (!(tree.dataStore() instanceof InMemoryStore dataStore)) {
            System.out.println("Cannot verifyNoOrphans -- no InMemoryStore");
            return;
        }

        // Must know the DataStore is a specific class that gives more insight to stored data!

        Set<TimeId> pageIds_fromDataStore = dataStore.allDataPageIds();
        Set<TimeId> pageIds_fromTree =
                tree.allDataPages().stream().map(DataPage::id).collect(toSet());

        assertThat(pageIds_fromDataStore.size(), is(pageIds_fromTree.size()));
        assertThat(pageIds_fromDataStore.containsAll(pageIds_fromTree), is(true));

        System.out.println("  PASSED -- There are no orphan DataPages");

        Set<TimeId> nodeIds_fromDataStore = dataStore.allNodeHeaderIds();
        Set<TimeId> nodeIds_fromTree =
                tree.allNodes().stream().map(NodeHeader::id).collect(toSet());

        assertThat(nodeIds_fromDataStore.size(), is(nodeIds_fromTree.size()));
        assertThat(nodeIds_fromDataStore.containsAll(nodeIds_fromTree), is(true));

        System.out.println("  PASSED -- There are no orphan NodeHeaders");
    }

    private static void verifyNumChildrenSetCorrectly(InternalTree<LatLong, String> tree) {

        // All leaf nodes have the correct size (numChildren = Tuple Count)
        tree.leafNodes().forEach(leaf -> {
            DataPage<LatLong, String> dataPage = tree.dataPageAt(leaf.id());
            assertThat(
                    "dataPage: " + dataPage.id() + " has " + dataPage.size() + " tuples but, the leafNodeHeader claims "
                            + leaf.numTuples() + " tuples",
                    dataPage.size(),
                    is(leaf.numTuples()));
        });

        System.out.println("  PASSED -- Leaf node's NodeHeader.numTuples() match DataPage.size()");
    }

    private static void verifyAllDataContained(
            List<Tuple<LatLong, String>> testData, InternalTree<LatLong, String> tree) {

        int n = testData.size();

        Set<TimeId> testIds = testData.stream().map(tuple -> tuple.id()).collect(toSet());
        Set<TimeId> treeIds = tree.tuples().stream().map(tuple -> tuple.id()).collect(toSet());

        if (testIds.size() > treeIds.size()) {
            Set<TimeId> missingIds = Sets.difference(treeIds, testIds);
            System.out.println("Tree is missing test data: " + missingIds);
            missingIds.forEach(id -> System.out.println("  tree is missing: " + id));
        }

        assertThat("All test data is somewhere in the tree", tree.tuples().size(), is(n));
        assertThat("ID sets have same size", testIds.size(), is(treeIds.size()));
        assertThat("These sets are the same", testIds.containsAll(treeIds), is(true));
        System.out.println("  PASSED -- All test data is in the tree");
    }

    private static <K, V> void verifyEveryLeafNodeHasDataPage(InternalTree<K, V> tree) {
        tree.leafNodes()
                .forEach(leaf -> assertThat(
                        "Missing DataPage!\nleafNode " + leaf + " has no DataPage",
                        tree.dataPageAt(leaf.id()),
                        notNullValue()));
    }

    static void verifyTreeNodeRadii(InternalTree<LatLong, String> dataStore) {

        DistanceMetric<LatLong> distMetric = dataStore.config().distMetric;

        // leaf nodes are always as small as possible
        for (NodeHeader<LatLong> leafNode : dataStore.leafNodes()) {
            DataPage<LatLong, String> dataPage = dataStore.dataPageAt(leafNode.id());

            double expectedRootRadius = distMetric.computeRadius(leafNode.center(), dataPage.keyList());

            assertThat(leafNode.radius(), is(expectedRootRadius));
        }

        System.out.println("  PASSED -- All leaf nodes have as small a radius as possible");

        System.out.println("Skipping: verify Inner Node radius");
    }

    private static void verifyAllDataInExactlyOneLeaf(
            List<Tuple<LatLong, String>> testData, InternalTree<LatLong, String> tree) {

        Set<TimeId> testDataIds = testData.stream().map(tuple -> tuple.id()).collect(toSet());

        // ALL the data should be in a leaf node
        Set<TimeId> idsFoundInLeafNodes = tree.leafNodes().stream()
                .map(node -> tree.dataPageAt(node.id()))
                .flatMap(tupleSet -> tupleSet.idSet().stream())
                .collect(toSet());

        assertThat("All data can be found in a leaf", testDataIds.size(), is(idsFoundInLeafNodes.size()));
        System.out.println("  PASSED -- All test data is in at least one leaf");

        // If a tuple is in two nodes this will show it
        int leafSizeSum =
                tree.leafNodes().stream().mapToInt(node -> node.numTuples()).sum();

        assertThat("No tuple is allowed in multiple leaf nodes", leafSizeSum, is(testDataIds.size()));
        System.out.println("  PASSED -- No test data is in multiple leaves (i.e. DataPages)");
    }

    private static void verifyNoTupleInMultipleLeaves(InternalTree<LatLong, String> tree) {

        // ALL the data should be in a leaf node
        Set<TimeId> idsFoundInLeafNodes = tree.leafNodes().stream()
                .map(node -> tree.dataPageAt(node.id()))
                .flatMap(tupleSet -> tupleSet.idSet().stream())
                .collect(toSet());

        int nodeSizeSum =
                tree.leafNodes().stream().mapToInt(node -> node.numTuples()).sum();
        int pageSizeSum =
                tree.allDataPages().stream().mapToInt(page -> page.size()).sum();

        assertThat(idsFoundInLeafNodes.size(), is(pageSizeSum));
        assertThat(idsFoundInLeafNodes.size(), is(nodeSizeSum));

        System.out.println("  PASSED -- No data is in multiple leaves (i.e. DataPages)");
    }

    private static void verifyNoDataAtInnerNodes(InternalTree<LatLong, String> tree) {

        // NO data should be in the inner nodes
        // Trying to get data from ANY inner node's ID should always yeild a null value
        tree.innerNodes().stream()
                .map(NodeHeader::id)
                .forEach(innerId -> assertThat(tree.dataPageAt(innerId), nullValue()));

        System.out.println("  PASSED -- No DataPage shares an id with an inner node");
    }

    private static void verifyInnerNodesHaveChildren(InternalTree<LatLong, String> tree) {
        tree.innerNodes()
                .forEach(innerNode -> assertThat(
                        "InnerNode's should have children: " + innerNode, innerNode.numChildren(), greaterThan(0)));

        System.out.println("  PASSED -- All Inner nodes have children");
    }

    private static <K, V> void verifyInnerNodeChildrenAreFound(InternalTree<K, V> tree) {

        tree.innerNodes().forEach(innerNode -> verifyChildrenExist(tree, innerNode));

        System.out.println("  PASSED -- Inner node's children can all be found");
    }

    private static <K, V> void verifyChildrenExist(InternalTree<K, V> tree, NodeHeader<K> innerNode) {

        List<NodeHeader<K>> children = tree.nodesBelow(innerNode.id());
        int numChildren = innerNode.numChildren();

        if (numChildren != children.size()) {
            System.out.println("Problem with inner node consistency found!");
            System.out.println("Inner node: " + innerNode);
            System.out.println("Nodes below: ...");
            children.forEach(node -> System.out.println(node));
        }

        assertThat("innerNode.numChildren: " + numChildren, children.size(), is(numChildren));
        assertThat(numChildren <= tree.config().branchingFactor, is(true));
    }

    private static void verifyLeafNodesObeyMaxTuplesPerNode(InternalTree<LatLong, String> tree) {

        int maxTuplesPerPage = tree.config().maxTuplesPerPage;

        tree.leafNodes().forEach(leaf -> assertThat(leaf.numTuples(), lessThanOrEqualTo(maxTuplesPerPage)));

        System.out.println("  PASSED -- No Leaf Node size exceeds: maxTuplesPerPage");
    }

    private static void verifyLeafNodesAndDataPageSizeMatches(InternalTree<LatLong, String> tree) {

        tree.leafNodes()
                .forEach(leaf -> assertThat(
                        leaf.id() + ": sizes must match",
                        leaf.numTuples(),
                        is(tree.dataPageAt(leaf.id()).size())));

        System.out.println("  PASSED -- Leaf Node size = Data Page size");
    }
}
