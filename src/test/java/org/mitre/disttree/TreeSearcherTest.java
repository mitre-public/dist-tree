package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.Serdes.latLongSerde;
import static org.mitre.disttree.Serdes.stringUtf8Serde;
import static org.mitre.disttree.stores.DataStores.inMemoryStore;

import org.mitre.caasd.commons.LatLong;

import org.junit.jupiter.api.Test;

class TreeSearcherTest {

    @Test
    public void canQueryEmptyTree() {

        TreeConfig<LatLong, String> config = TreeConfig.<LatLong, String>builder()
                .distMetric((a, b) -> a.distanceTo(b).inNauticalMiles())
                .dataStore(inMemoryStore())
                .keySerde(latLongSerde())
                .valueSerde(stringUtf8Serde())
                .build();

        InternalTree<LatLong, String> dataStore = new InternalTree<>(config);

        TreeSearcher<LatLong, String> searcher = new TreeSearcher<>(dataStore);

        LatLong searchKey = randomLatLong();
        double searchRadiusNm = 500;
        SearchResults<LatLong, String> rangeResult = searcher.getAllWithinRange(searchKey, searchRadiusNm);
        SearchResults<LatLong, String> knnResult = searcher.getClosest(searchKey);

        assertThat(rangeResult.isEmpty(), is(true));
        assertThat(knnResult.isEmpty(), is(true));
    }
}
