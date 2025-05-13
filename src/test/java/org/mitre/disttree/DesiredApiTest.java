package org.mitre.disttree;

import static java.util.Collections.unmodifiableList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mitre.caasd.commons.fileutil.FileUtils.deleteDirectory;
import static org.mitre.disttree.Batch.batchify;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.stringUtf8Serde;
import static org.mitre.disttree.SharedTestUtils.*;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.mitre.caasd.commons.LatLong;
import org.mitre.disttree.Serdes.LatLongSerde;
import org.mitre.disttree.Serdes.StringUtf8Serde;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DesiredApiTest {

    // All test use the same dataset
    final List<Tuple<LatLong, String>> testData = unmodifiableList(createTestData(5_000));

    @Test
    public void easyKeyValuePairWriting() {
        /*
         * This "test" shows a simple code snippet that writes K/V paris to a DistanceTree.
         *
         * Basic behavior correctness is verified.
         */
        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(300)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(new LatLongSerde())
                .valueSerde(new StringUtf8Serde())
                .build();

        DistanceTree<LatLong, String> tree = new DistanceTree<>(config);

        // Create Batches of 250 Key-Value Pairs (e.g. Tuples)
        List<Batch<LatLong, String>> batches = Batch.batchify(testData, 250);

        // Write each batch
        batches.forEach(tree::addBatch);

        verifyTree(testData, tree);
    }

    @Test
    public void evenEasierKeyValuePairWriting() {

        Map<LatLong, String> testMap = createTestMap(4_567);

        /*
         * This "test" shows a more abbreviated snippet that writes a Map of data to a DistanceTree.
         *
         * Basic behavior correctness is NOT verified because we aren't making and retaining the
         * Tuples (which are required for correctness testing)
         */
        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(300)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(new LatLongSerde())
                .valueSerde(new StringUtf8Serde())
                .build();

        DistanceTree<LatLong, String> tree = new DistanceTree<>(config);

        // Create batches of 250 Key-Value Pairs and immediately add them to the tree (not manually creating tuples)
        tree.addBatches(Batch.batchify(testMap, 250));

        assertThat(tree.treeStats().numTuples(), is(4_567));
    }

    @Test
    public void easySearching() {
        /*
         * This "test" shows a simple code snippet that queries a DistanceTree.
         *
         * Basic behavior correctness is verified.
         */
        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(50)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .build();

        DistanceTree<LatLong, String> tree = new DistanceTree<>(config);

        // We only add data to the tree because the configuration points to an empty in-memory data store
        List<Batch<LatLong, String>> batches = batchify(testData, 50);
        batches.forEach(batch -> tree.addBatch(batch));

        // Query a DistanceTree
        LatLong searchKey = randomLatLong();
        double searchRadiusNm = 500;
        SearchResults<LatLong, String> result = tree.rangeSearch(searchKey, searchRadiusNm);

        // Verify Query results are correct
        List<LatLong> manualResults = findTestDataNear(searchKey, searchRadiusNm);
        assertThat(manualResults.size(), is(result.size()));
        result.results().forEach(searchResult -> assertThat(manualResults.contains(searchResult.key()), is(true)));
    }

    private List<LatLong> findTestDataNear(LatLong searchKey, double range) {
        return testData.stream()
                .filter(entry -> searchKey.distanceInNM(entry.key()) < range)
                .map(entry -> entry.key())
                .toList();
    }

    @Test
    @Order(1)
    void simplestPossibleExample_part1() {
        // This code should appear in the README as the simplest possible working example

        // Building a DistanceTree starts with a TreeConfig
        DistanceTree<LatLong, String> tree = TreeConfig.<LatLong, String>builder()
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .buildTree();

        // Gather the Key-Value data we want to store in the tree
        List<LatLong> locations = loadBusinessLocations();
        List<String> names = loadBusinessNames();
        tree.addBatches(batchify(locations, names, 1_000));

        // Immediately search the dataset
        LatLong searchKey = randomLatLong();
        SearchResults<LatLong, String> fiveClosestBusinesses = tree.knnSearch(searchKey, 5);

        // --- JVM is shutdown, but our dataset is persisted ---
    }

    @Test
    @Order(2)
    void simplestPossibleExample_part2() {

        // The next JVM constructs the tree
        DistanceTree<LatLong, String> tree = TreeConfig.<LatLong, String>builder()
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .buildTree();

        // No data loading is necessary
        // The default config saves/loads data to a known disk location

        // Search the dataset that was indexed by the prior JVM
        LatLong searchKey = randomLatLong();
        double searchRadiusNm = 0.5;
        SearchResults<LatLong, String> businessesNearby = tree.rangeSearch(searchKey, searchRadiusNm);
    }

    static List<LatLong> loadBusinessLocations() {
        int n = 1000;
        return Stream.generate(MiscTestUtils::randomLatLong).limit(n).toList();
    }

    static List<String> loadBusinessNames() {
        int n = 1000;
        return IntStream.range(0, n).mapToObj(i -> Integer.toString(i)).toList();
    }

    @AfterAll
    public static void deleteDuckDbDir() throws IOException {
        File duckDbDir = new File("DuckDBStore");

        deleteDirectory(duckDbDir);
    }
}
