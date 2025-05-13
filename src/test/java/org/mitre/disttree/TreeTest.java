package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.stringUtf8Serde;
import static org.mitre.disttree.SharedTestUtils.addTestDataToTree;
import static org.mitre.disttree.SharedTestUtils.verifyTree;
import static org.mitre.disttree.Tuple.newTuple;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.mitre.caasd.commons.LatLong;

import org.junit.jupiter.api.Test;

public class TreeTest {

    @Test
    public void canAddLotsOfDataWithSameKey() {

        /*
         * Filling the tree with data that has the same Key should not fail
         */

        int branchingFactor = 2;
        int maxEntriesPerPage = 250;
        int size = 250_000;

        LatLong theSoleKey = randomLatLong();

        DistanceTree<LatLong, String> tree = TreeConfig.<LatLong, String>builder()
                .branchingFactor(branchingFactor)
                .maxTuplesPerPage(maxEntriesPerPage)
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .buildTree();

        // add some data to the Tree
        List<Tuple<LatLong, String>> testData = createTestData(size, theSoleKey);
        addTestDataToTree(tree, testData);

        // The tree is still correctly formatted (after all operations)
        verifyTree(testData, tree);

        // And Queries still return the correct results
        SearchResults<LatLong, String> results = tree.rangeSearch(theSoleKey, 0.1);

        assertThat(results.size(), is(size));

        var testIds = testData.stream().map(entry -> entry.id()).collect(Collectors.toSet());
        var resultIds = results.ids();

        assertThat(testIds.size(), is(resultIds.size()));
    }

    /** Create n Entries that all have the same LatLong Key. */
    private static List<Tuple<LatLong, String>> createTestData(int n, LatLong sharedKey) {

        return IntStream.range(0, n)
                .mapToObj(i -> newTuple(sharedKey, Integer.toString(i)))
                .toList();
    }
}
