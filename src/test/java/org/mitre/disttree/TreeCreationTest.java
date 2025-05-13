package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mitre.disttree.Batch.batchify;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.Serdes.*;
import static org.mitre.disttree.SharedTestUtils.*;
import static org.mitre.disttree.Tuple.newSlimTuple;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.util.ArrayList;
import java.util.List;

import org.mitre.caasd.commons.LatLong;

import org.junit.jupiter.api.Test;

public class TreeCreationTest {

    static final DistanceMetric<LatLong> METRIC = (a, b) -> a.distanceTo(b).inNauticalMiles();
    static final Serde<LatLong> KEY_SERDE = latLongSerde();
    static final Serde<String> VALUE_SERDE = stringUtf8Serde();

    @Test
    public void simplestPossibleSpinUp_rootNeverSplits() {

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(64)
                .distMetric(METRIC)
                .dataStore(inMemoryStore())
                .keySerde(KEY_SERDE)
                .valueSerde(VALUE_SERDE)
                .build();

        DistanceTree<LatLong, String> tree = new DistanceTree<>(config);

        List<Tuple<LatLong, String>> testData = createTestData(10); // 10 < 64 tuples ... never splitting root
        addTestDataToTree(tree, testData);

        verifyTree(testData, tree);

        assertThat("Tree has one inner node", tree.treeStats().numInnerNodes(), is(1));
        assertThat("Tree has one leaf node", tree.treeStats().numLeafNodes(), is(1));
    }

    @Test
    public void demoSimplestPossibleSpinUp_thatSplitsLeafNode() {

        int MAX_ENTRIES_PER_NODE = 8;
        int DATASET_SIZE = 9;

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(MAX_ENTRIES_PER_NODE)
                .distMetric(METRIC)
                .dataStore(inMemoryStore())
                .keySerde(KEY_SERDE)
                .valueSerde(VALUE_SERDE)
                .build();

        var tree = new InternalTree<>(config);

        List<Tuple<LatLong, String>> testData = createTestData(DATASET_SIZE);
        addTestDataToTree(tree, testData);

        verifyTree(testData, tree);

        assertThat("Tree has one inner node", tree.treeStats().numInnerNodes(), is(1));
    }

    @Test
    public void demoSimplestPossibleSpinUp_thatSplitsInnerNode() {

        // Storing 19 items requires at least 4 leaf nodes (given this config)
        // This will split the first inner node

        int MAX_ENTRIES_PER_NODE = 5;
        int DATASET_SIZE = 19;

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(MAX_ENTRIES_PER_NODE)
                .branchingFactor(3)
                .distMetric(METRIC)
                .dataStore(inMemoryStore())
                .keySerde(KEY_SERDE)
                .valueSerde(VALUE_SERDE)
                .build();

        var tree = new InternalTree<>(config);

        List<Tuple<LatLong, String>> testData = createTestData(DATASET_SIZE);
        addTestDataToTree(tree, testData);

        verifyTree(testData, tree);
    }

    @Test
    public void testLargeInMemoryTree() {

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(500)
                .branchingFactor(20)
                .distMetric(METRIC)
                .dataStore(inMemoryStore())
                .keySerde(KEY_SERDE)
                .valueSerde(VALUE_SERDE)
                .build();

        // The DataStore contains enough information to convert "nicely typed key value data" into
        // byte[]s and delegate reading and writing those bytes to the DataStore.

        var tree = new InternalTree<>(config);

        // Create a dataset of 1k items,
        List<Tuple<LatLong, String>> testData = createTestData(10_000);

        // put all testData in the tree
        addTestDataToTree(tree, testData);

        verifyTree(testData, tree);
        System.out.println(tree.treeStats().toString());
    }

    @Test
    public void testTreeWithNullValues() {

        // Here we build an DistanceTree where we never provide the "Value V" Strings

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(500)
                .branchingFactor(20)
                .distMetric(METRIC)
                .dataStore(inMemoryStore())
                .keySerde(KEY_SERDE)
                .valueSerde(VALUE_SERDE)
                .build();

        // The DataStore contains enough information to convert "nicely typed key value data" into
        // byte[]s and delegate reading and writing those bytes to the DataStore.

        var tree = new InternalTree<>(config);

        // Create a dataset of 1k items,
        List<Tuple<LatLong, String>> testData = createTestDataWithNullValues(1_000);

        // put all testData in the tree
        addTestDataToTree(tree, testData);

        verifyTree(testData, tree);
        System.out.println(tree.treeStats().toString());
    }

    @Test
    public void testTreeWithUnusedValue() {

        // Here we build a DistanceTree where we make it clear we never use values via the signature

        TreeConfig<LatLong, Void> config = TreeConfig.<LatLong, Void>builder()
                .maxTuplesPerPage(500)
                .branchingFactor(20)
                .distMetric(METRIC)
                .dataStore(inMemoryStore())
                .keySerde(KEY_SERDE)
                .valueSerde(voidSerde())
                .build();

        // The DataStore contains enough information to convert "nicely typed key value data" into
        // byte[]s and delegate reading and writing those bytes to the DataStore.

        var internalTree = new InternalTree<>(config);

        // Create a dataset of 1k items,

        List<Tuple<LatLong, Void>> testData = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            testData.add(newSlimTuple(randomLatLong()));
        }

        // put all testData in the tree
        var tree = new DistanceTree<>(internalTree);
        List<Batch<LatLong, Void>> batches = batchify(testData, 50);
        batches.forEach(batch -> tree.addBatch(batch));

        // verify a search can run
        assertDoesNotThrow(() -> tree.getClosest(randomLatLong()));
    }
}
