package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mitre.disttree.MiscTestUtils.newRandomDatum;

import org.mitre.caasd.commons.LatLong;

import org.junit.jupiter.api.Test;

class BatchAccumulatorTest {

    @Test
    public void itemsInBatchAreOrdered() {

        var batchMaker = new Batch.BatchAccumulator<LatLong, byte[]>();

        Tuple<LatLong, byte[]> tuple1 = newRandomDatum();
        Tuple<LatLong, byte[]> tuple2 = newRandomDatum();
        Tuple<LatLong, byte[]> tuple3 = newRandomDatum();
        Tuple<LatLong, byte[]> tuple4 = newRandomDatum();

        batchMaker.addToBatch(tuple1);
        batchMaker.addToBatch(tuple2);
        batchMaker.addToBatch(tuple3);
        batchMaker.addToBatch(tuple4);

        Batch<LatLong, byte[]> batch = batchMaker.drainToBatch();

        assertThat(batch.tuples().get(0), is(tuple1));
        assertThat(batch.tuples().get(1), is(tuple2));
        assertThat(batch.tuples().get(2), is(tuple3));
        assertThat(batch.tuples().get(3), is(tuple4));
    }

    @Test
    public void drainEmptiesBatchAccumulator() {

        var batchMaker = new Batch.BatchAccumulator<LatLong, byte[]>();

        assertThat(batchMaker.currentBatchSize(), is(0));

        batchMaker.addToBatch(newRandomDatum());
        batchMaker.addToBatch(newRandomDatum());
        batchMaker.addToBatch(newRandomDatum());
        batchMaker.addToBatch(newRandomDatum());

        assertThat(batchMaker.currentBatchSize(), is(4));

        Batch<LatLong, byte[]> batch = batchMaker.drainToBatch();

        assertThat(batchMaker.currentBatchSize(), is(0));
        assertThat(batch.tuples().size(), is(4));
    }
}
