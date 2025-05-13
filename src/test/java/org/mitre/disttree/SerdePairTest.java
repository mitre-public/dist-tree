package org.mitre.disttree;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mitre.disttree.Serdes.*;

import java.util.List;
import java.util.stream.IntStream;

import org.mitre.caasd.commons.LatLong;
import org.mitre.caasd.commons.ids.TimeId;

import org.junit.jupiter.api.Test;

class SerdePairTest {

    @Test
    public void singleTupleSerdeLoop() {
        // Test the serialize-deserialize loop for individual Tuples
        var serdePair = new SerdePair<>(latLongSerde(), stringUtf8Serde());

        Tuple<LatLong, String> inTuple = new Tuple<>(TimeId.newId(), LatLong.of(0.5, 10.0), "hello");

        Tuple<byte[], byte[]> asByteTuple = serdePair.serialize(inTuple);

        Tuple<LatLong, String> outTuple = serdePair.deserialize(asByteTuple);

        assertThat(inTuple.id(), is(outTuple.id()));
        assertThat(inTuple, is(outTuple));
        assertThat(inTuple == outTuple, is(false));
    }

    @Test
    public void tupleListSerdeLoop() {
        // Test the serialize-deserialize loop for lists of Tuples
        var serdePair = new SerdePair<>(latLongSerde(), stringUtf8Serde());

        List<Tuple<LatLong, String>> inList = List.of(
                new Tuple<>(TimeId.newId(), LatLong.of(0.5, 10.0), "hello"),
                new Tuple<>(TimeId.newId(), LatLong.of(1.5, 11.0), "goodbye"));

        List<Tuple<byte[], byte[]>> asByteTuple = serdePair.serialize(inList);

        List<Tuple<LatLong, String>> outList = serdePair.deserialize(asByteTuple);

        assertThat(inList, hasSize(outList.size()));
        IntStream.range(0, 2).forEach(i -> assertThat(inList.get(0), is(outList.get(0))));
    }
}
