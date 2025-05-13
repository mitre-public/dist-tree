package org.mitre.disttree;

import static java.util.stream.Stream.generate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.voidSerde;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.util.List;

import org.mitre.caasd.commons.LatLong;

import org.junit.jupiter.api.Test;

class DistanceTreeTest {

    @Test
    void distanceMetricCountsAreCorrect() {

        /*
         * In this test we MANUALLY create a DistanceMetric that counts how many times it is called.
         *
         * Then we verify the manual callCount equals the automatically computed callCount (ie the
         * count relying on internally decorating the raw DistanceMetric with counting behavior)
         */

        ManualCountingDM metric = new ManualCountingDM();

        TreeConfig<LatLong, Void> config = TreeConfig.<LatLong, Void>builder()
                .maxTuplesPerPage(300)
                .branchingFactor(4) // lower branching = fewer Distance Metric calls
                .distMetric(metric)
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(voidSerde())
                .build();

        DistanceTree<LatLong, Void> tree = new DistanceTree<>(config);

        List<LatLong> testData =
                generate(MiscTestUtils::randomLatLong).limit(5000).toList();

        Batch.batchifyKeys(testData, 250).forEach(tree::addBatch);

        assertThat(tree.distMetricExecutionCount(), is(metric.callCount));
        assertThat(tree.treeStats().numTuples(), is(5000));
    }

    static class ManualCountingDM implements DistanceMetric<LatLong> {

        long callCount = 0;

        @Override
        public double distanceBtw(LatLong item1, LatLong item2) {
            callCount++;
            return item1.distanceTo(item2).inNauticalMiles();
        }
    }
}
