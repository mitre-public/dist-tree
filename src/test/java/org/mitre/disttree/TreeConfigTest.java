package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mitre.disttree.Batch.batchifyKeys;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.voidSerde;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.util.List;
import java.util.stream.Stream;

import org.mitre.caasd.commons.LatLong;

import com.google.common.math.StatsAccumulator;
import org.junit.jupiter.api.Test;

class TreeConfigTest {

    @Test
    public void canMakeConfig() {

        Serde<LatLong> keySerde = new Serdes.LatLongSerde();
        Serde<String> valueSerde = new Serdes.StringUtf8Serde();
        DistanceMetric<LatLong> metric = (a, b) -> a.distanceTo(b).inNauticalMiles();

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(8)
                .branchingFactor(5)
                .keySerde(keySerde)
                .valueSerde(valueSerde)
                .distMetric(metric)
                .dataStore(inMemoryStore())
                .build();

        assertThat(config.branchingFactor(), is(5));
        assertThat(config.maxTuplesPerPage(), is(8));
        assertThat(config.serdePair().keySerde(), is(keySerde));
        assertThat(config.serdePair().valueSerde(), is(valueSerde));
        assertThat(config.distMetric().innerMetric(), is(metric));
    }

    @Test
    public void repackBehaviorIsRespected() {

        StatsAccumulator simpleTreeOpCounts = new StatsAccumulator();
        StatsAccumulator repackingTreeOpCounts = new StatsAccumulator();

        int BRANCHING_FACTOR = 5;

        int NUM_TRIALS = 10;
        for (int i = 0; i < NUM_TRIALS; i++) {

            DistanceTree<LatLong, Void> simpleTree = TreeConfig.<LatLong, Void>builder()
                    .noRepacking()
                    .maxTuplesPerPage(100)
                    .branchingFactor(BRANCHING_FACTOR)
                    .keySerde(latLongSerde())
                    .valueSerde(voidSerde())
                    .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                    .dataStore(inMemoryStore())
                    .buildTree();

            DistanceTree<LatLong, Void> repackingTree = TreeConfig.<LatLong, Void>builder()
                    .incrementalRepacking()
                    .maxTuplesPerPage(100)
                    .branchingFactor(BRANCHING_FACTOR)
                    .keySerde(latLongSerde())
                    .valueSerde(voidSerde())
                    .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                    .dataStore(inMemoryStore())
                    .buildTree();

            // create some test data
            int N = 1000;
            int BATCH_SIZE = 50;
            List<LatLong> data =
                    Stream.generate(MiscTestUtils::randomLatLong).limit(N).toList();
            List<Batch<LatLong, Void>> batches = batchifyKeys(data, BATCH_SIZE);

            batches.forEach(b -> simpleTree.addBatch(b));
            batches.forEach(b -> repackingTree.addBatch(b));

            // Record how often each distance metric is called, repacking should increase the count
            simpleTreeOpCounts.add(simpleTree.distMetricExecutionCount());
            repackingTreeOpCounts.add(repackingTree.distMetricExecutionCount());
        }

        System.out.println();
        System.out.println(simpleTreeOpCounts.snapshot());
        System.out.println(repackingTreeOpCounts.snapshot());

        // mean = 18,670 & sigma = 1,366   <--- very rough numbers
        assertThat(14_572 < simpleTreeOpCounts.mean(), is(true));
        assertThat(simpleTreeOpCounts.mean() < 22_768, is(true));

        // mean = 29,842 & sigma = 4,405  <--- very rough numbers
        assertThat(16_627 < repackingTreeOpCounts.mean(), is(true));
        assertThat(repackingTreeOpCounts.mean() < 43_057, is(true));

        assertThat(simpleTreeOpCounts.mean() < repackingTreeOpCounts.mean(), is(true));
    }

    @Test
    public void readWriteBehaviorIsRespected() {

        // create some test data
        int N = 100;
        int BATCH_SIZE = 20;
        List<LatLong> data =
                Stream.generate(MiscTestUtils::randomLatLong).limit(N).toList();
        List<Batch<LatLong, Void>> batches = batchifyKeys(data, BATCH_SIZE);

        // Cannot accept data
        DistanceTree<LatLong, Void> readTree = TreeConfig.<LatLong, Void>builder()
                .readOnly()
                .keySerde(latLongSerde())
                .valueSerde(voidSerde())
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .buildTree();

        assertThrows(UnsupportedOperationException.class, () -> readTree.addBatch(batches.get(0)));

        // Cannot run queries
        DistanceTree<LatLong, Void> writeTree = TreeConfig.<LatLong, Void>builder()
                .writeOnly()
                .keySerde(latLongSerde())
                .valueSerde(voidSerde())
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .buildTree();

        assertDoesNotThrow(() -> writeTree.addBatch(batches.get(0)));
        assertThrows(UnsupportedOperationException.class, () -> writeTree.getClosest(randomLatLong()));

        // Accepts both reads and write
        DistanceTree<LatLong, Void> readWriteTree = TreeConfig.<LatLong, Void>builder()
                .readAndWrite()
                .keySerde(latLongSerde())
                .valueSerde(voidSerde())
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .buildTree();

        assertDoesNotThrow(() -> readWriteTree.addBatch(batches.get(0)));
        assertDoesNotThrow(() -> readWriteTree.getClosest(randomLatLong()));
    }
}
