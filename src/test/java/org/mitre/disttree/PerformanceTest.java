package org.mitre.disttree;

import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.stringUtf8Serde;
import static org.mitre.disttree.SharedTestUtils.createTestData;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.time.Duration;
import java.util.List;

import org.mitre.caasd.commons.LatLong;
import org.mitre.caasd.commons.util.SingleUseTimer;

import com.google.common.math.StatsAccumulator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class PerformanceTest {

    /**
     * This is "the seed" of a Suite of Performance Tests (hopefully)
     * <p>
     * This unit test measures: The time it takes to build the tree, The number of times the
     * DistanceMetric is called, How big the resulting Leaf Nodes are.
     * <p>
     * We can extend this example to study the impact of various tree parameters
     */
    @Disabled
    @Test
    public void analyzePerformance() {

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .incrementalRepacking()
                .maxTuplesPerPage(500)
                .branchingFactor(4)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .build();

        int NUM_TRIALS = 100;
        int NUM_TUPLES_IN_TREE = 10_000;

        measureTree(config, NUM_TRIALS, NUM_TUPLES_IN_TREE);
    }

    private void measureTree(TreeConfig<LatLong, String> config, int numTrials, int treeSize) {

        SingleUseTimer timer = new SingleUseTimer();
        timer.tic();

        StatsAccumulator distMetricCalls = new StatsAccumulator();
        StatsAccumulator avgRadius = new StatsAccumulator();

        for (int i = 0; i < numTrials; i++) {
            DistanceTree<LatLong, String> tree = createTree(config, treeSize);

            TreeStats stats = tree.treeStats();

            distMetricCalls.add(tree.distMetricExecutionCount());
            avgRadius.add(stats.meanPageRadius());
        }

        timer.toc();

        System.out.println("Avg Distance Metric Calls: " + distMetricCalls.mean());
        System.out.println("Avg Avg Radius: " + avgRadius.mean());

        Duration duration = timer.elapsedTime();
        System.out.println("Execution time: " + duration.getSeconds() + " seconds");
    }

    /** Build a DistanceTree, fill it with random LatLong data. */
    private DistanceTree<LatLong, String> createTree(TreeConfig<LatLong, String> config, int numTuples) {

        DistanceTree<LatLong, String> tree = new DistanceTree<>(config);

        // Create a dataset of 10k items,
        List<Tuple<LatLong, String>> testData = createTestData(numTuples);

        // Add it to the tree
        List<Batch<LatLong, String>> batches = Batch.batchify(testData, 250);
        batches.forEach(tree::addBatch);

        return tree;
    }
}
