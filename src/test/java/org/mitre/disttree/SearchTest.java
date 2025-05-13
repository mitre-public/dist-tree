package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mitre.disttree.Batch.batchify;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.stringUtf8Serde;
import static org.mitre.disttree.SharedTestUtils.createTestData;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.awt.Color;
import java.io.File;
import java.util.Collection;
import java.util.List;

import org.mitre.caasd.commons.Distance;
import org.mitre.caasd.commons.LatLong;
import org.mitre.caasd.commons.maps.MapBuilder;
import org.mitre.caasd.commons.maps.MapFeatures;
import org.mitre.caasd.commons.maps.MonochromeTileServer;

import org.junit.jupiter.api.Test;

class SearchTest {

    final boolean PLOT_SEARCH_DATA_AND_RESULTS = false;

    @Test
    void demonstrateSearch() {

        List<Tuple<LatLong, String>> testData = createTestData(1_000);
        InternalTree<LatLong, String> tree = makeInMemoryTreeWithData(testData);

        // Search Goal = Find K-nearest Neighbors of this LatLong point
        LatLong searchKey = randomLatLong();
        final int K = 4;

        Search<LatLong, String> search = Search.knnSearch(searchKey, K, tree);
        search.executeQuery();

        SearchResults<LatLong, String> results = search.results();

        // Found K neighbors, result(0) is the closest neighbor
        assertThat(results.size(), is(K));
        assertThat(results.result(0).distance(), is(computeMinimumDistance(searchKey, testData)));

        if (PLOT_SEARCH_DATA_AND_RESULTS) {
            MapBuilder.newMapBuilder()
                    .tileSource(new MonochromeTileServer(Color.BLACK))
                    .center(LatLong.of(0.0, 0.0)) // the center of the random distribution LatLongs are drawn from
                    .width(3200, 6)
                    .addFeatures(testData, tuple -> MapFeatures.filledCircle(tuple.key(), Color.RED, 10))
                    .addFeatures(results.tuples(), tuple -> MapFeatures.filledCircle(tuple.key(), Color.CYAN, 10))
                    .addFeatures(MapFeatures.filledCircle(searchKey, Color.MAGENTA, 14))
                    .toFile(new File("demonstrateSearch.png"));
        }
    }

    private double computeMinimumDistance(LatLong searchKey, List<Tuple<LatLong, String>> testData) {

        Distance smallestDist = testData.stream()
                .map(entry -> entry.key().distanceTo(searchKey))
                .sorted()
                .findFirst()
                .orElseThrow();

        return smallestDist.inNauticalMiles();
    }

    @Test
    public void mustExecuteSearchToGetResults() {

        InternalTree<LatLong, String> tree = makeInMemoryTree();

        LatLong searchKey = randomLatLong();

        Search<LatLong, String> search = Search.knnSearch(searchKey, 2, tree);

        Throwable thrown = assertThrows(IllegalStateException.class, () -> search.results());
        assertThat(thrown.getMessage(), is("Search was not executed"));
    }

    InternalTree<LatLong, String> makeInMemoryTreeWithData(Collection<Tuple<LatLong, String>> data) {

        InternalTree<LatLong, String> tree = makeInMemoryTree();

        // Use a TreeFacade to "unlock" the ability to add data to the tree
        var facade = new DistanceTree<>(tree);

        List<Batch<LatLong, String>> batches = batchify(data, 50);
        batches.forEach(batch -> facade.addBatch(batch));

        return tree;
    }

    InternalTree<LatLong, String> makeInMemoryTree() {

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .build();

        return new InternalTree<>(config);
    }
}
