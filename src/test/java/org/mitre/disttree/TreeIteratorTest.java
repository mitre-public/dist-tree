package org.mitre.disttree;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mitre.disttree.SharedTestUtils.*;
import static org.mitre.disttree.TreeCreationTest.*;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.mitre.caasd.commons.LatLong;
import org.mitre.caasd.commons.ids.TimeId;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TreeIteratorTest {

    @Test
    public void testTreeIterator() {

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(100)
                .branchingFactor(20)
                .distMetric(METRIC)
                .dataStore(inMemoryStore())
                .keySerde(KEY_SERDE)
                .valueSerde(VALUE_SERDE)
                .build();

        // The DataStore contains enough information to convert "nicely typed key value data" into
        // byte[]s and delegate reading and writing those bytes to the DataStore.

        var tree = new InternalTree<>(config);

        // Create a dataset of 10k items,
        List<Tuple<LatLong, String>> testData = createTestData(10_000);

        // put all testData in the tree
        addTestDataToTree(tree, testData);

        verifyTree(testData, tree);

        Set<TimeId> leafIds = tree.leafNodes().stream().map(leaf -> leaf.id()).collect(toSet());
        Set<TimeId> foundIds = new TreeSet<>();
        int totalSize = 0;

        TreeIterator<LatLong, String> iter = new TreeIterator<>(tree);

        while (iter.hasNext()) {
            DataPage<LatLong, String> page = iter.next();

            foundIds.add(page.id());
            totalSize += page.size();
        }

        assertThat(totalSize, is(10_000));
        assertThat(Sets.difference(leafIds, foundIds).size(), is(0));
    }

    @Test
    public void verifyTreeIteratorDetectsMutation() {

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .maxTuplesPerPage(20)
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
        List<Tuple<LatLong, String>> testData = createTestData(1_000);

        // put all testData in the tree
        addTestDataToTree(tree, testData);

        verifyTree(testData, tree);

        TreeIterator<LatLong, String> iter = new TreeIterator<>(tree);
        iter.next();
        iter.next();

        addTestDataToTree(tree, createTestData(100));

        assertThrows(ConcurrentModificationException.class, iter::next);
    }
}
