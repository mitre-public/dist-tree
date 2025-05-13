package org.mitre.disttree;

import static org.mitre.disttree.Batch.batchify;
import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.stringUtf8Serde;
import static org.mitre.disttree.SharedTestUtils.createTestData;
import static org.mitre.disttree.SharedTestUtils.verifyTree;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.util.ArrayList;
import java.util.List;

import org.mitre.caasd.commons.LatLong;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;

@Tag("SLOW")
public class StressTest {

    @RepeatedTest(100)
    public void batchByBatchTreeBuild() {

        /*
         * This test is slow, but powerful.
         *
         * It helps tracks down failures that occur while building a tree (without repacks).
         * It verifies the Tree after every batch is sent (e.g. each TreeTransaction occurs).
         *
         * So, if your tree state becomes invalid -- this will help you find the transaction that
         * yields and invalid state.
         */

        int SIZE_OF_TEST_SIZE = 10_000;

        int BATCH_SIZE = 200;
        int MAX_ENTRIES_PER_NODE = 75;

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(MAX_ENTRIES_PER_NODE)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .build();

        var tree = new InternalTree<>(config);

        DistanceTree<LatLong, String> facade = new DistanceTree<>(tree);

        List<Tuple<LatLong, String>> testData = createTestData(SIZE_OF_TEST_SIZE);
        List<Batch<LatLong, String>> batches = batchify(testData, BATCH_SIZE);

        List<Tuple<LatLong, String>> dataSoFar = new ArrayList<>(SIZE_OF_TEST_SIZE);

        for (Batch<LatLong, String> batch : batches) {
            facade.addBatch(batch);

            dataSoFar.addAll(batch.tuples());

            verifyTree(dataSoFar, tree);
        }
    }
}
