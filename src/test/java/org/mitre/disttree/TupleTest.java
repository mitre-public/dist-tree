package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mitre.disttree.Tuple.newTuple;

import org.mitre.caasd.commons.LatLong;

import org.junit.jupiter.api.Test;

class TupleTest {

    @Test
    public void canMakeSimpleKvPairs() {

        Tuple<LatLong, byte[]> treeTuple = newTuple(LatLong.of(0.0, 1.0), new byte[] {15, 16});

        assertThat(treeTuple.key().latitude(), is(0.0));
        assertThat(treeTuple.key().longitude(), is(1.0));
        assertThat(treeTuple.value()[0], is((byte) 15));
        assertThat(treeTuple.value()[1], is((byte) 16));
    }
}
