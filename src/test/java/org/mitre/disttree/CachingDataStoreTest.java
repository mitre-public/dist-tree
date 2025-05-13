// package org.mitre.disttree;
//
// import static org.mitre.disttree.Batch.batchify;
// import static org.mitre.disttree.Serdes.latLongSerde;
// import static org.mitre.disttree.Serdes.stringUtf8Serde;
// import static org.mitre.disttree.SharedTestUtils.*;
//
// import java.util.ArrayList;
// import java.util.List;
//
// import org.mitre.caasd.commons.LatLong;
//
// import org.junit.jupiter.api.RepeatedTest;
// import org.junit.jupiter.api.Tag;
//
// @Tag("SLOW")
// class CachingDataStoreTest {
//
//    @RepeatedTest(25)
//    public void batchByBatchTreeBuild() {
//
//        /*
//         * This test replicates a minor stress test while using a CachingDataStore instead of a
//         * DataStore.
//         *
//         * The goal of this test is to help us verify correctness of the caching before
//         * migrating the caching to the default implementation
//         */
//
//        int SIZE_OF_TEST_SIZE = 10_000;
//
//        int BATCH_SIZE = 200;
//        int MAX_ENTRIES_PER_NODE = 75;
//
//        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
//                .maxTuplesPerPage(MAX_ENTRIES_PER_NODE)
//                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
//                .dataStore(new CachingDataStore(new InMemoryStore()))
//                .keySerde(latLongSerde())
//                .valueSerde(stringUtf8Serde())
//                .build();
//
//        var tree = new Internaldisttree<>(config);
//
//        disttree<LatLong, String> facade = new disttree<>(tree);
//
//        List<Tuple<LatLong, String>> testData = createTestData(SIZE_OF_TEST_SIZE);
//        List<Batch<LatLong, String>> batches = batchify(testData, BATCH_SIZE);
//
//        List<Tuple<LatLong, String>> dataSoFar = new ArrayList<>(SIZE_OF_TEST_SIZE);
//
//        for (Batch<LatLong, String> batch : batches) {
//            facade.addBatch(batch);
//
//            dataSoFar.addAll(batch.tuples());
//
//            verifyTree(dataSoFar, tree);
//        }
//    }
// }
