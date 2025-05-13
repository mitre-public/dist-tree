package org.mitre.disttree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mitre.caasd.commons.ids.TimeId.newId;
import static org.mitre.disttree.MiscTestUtils.randomLatLong;
import static org.mitre.disttree.NodeHeader.newInnerNodeHeader;
import static org.mitre.disttree.NodeHeader.newLeafNodeHeader;
import static org.mitre.disttree.Ops.NodeOp.*;
import static org.mitre.disttree.Tuple.newTuple;

import java.util.List;

import org.mitre.caasd.commons.LatLong;
import org.mitre.caasd.commons.ids.TimeId;
import org.mitre.disttree.Ops.NodeOp;
import org.mitre.disttree.Ops.TupleOp;

import org.junit.jupiter.api.Test;

class OpsTest {

    @Test
    void newLeafHeader() {

        TimeId id = newId();
        TimeId parent = newId();
        LatLong center = randomLatLong();
        double radius = 5.0;
        int numLeafTuples = 5;

        NodeHeader<LatLong> leafNode = newLeafNodeHeader(id, parent, center, radius, numLeafTuples);

        assertThat(leafNode.isLeafNode(), is(true));
        assertThat(leafNode.isInnerNode(), is(false));

        assertThrows(UnsupportedOperationException.class, () -> leafNode.numChildren());
    }

    @Test
    void addTupleToLeaf_tupleCount() {

        TimeId id = newId();
        TimeId parent = newId();
        LatLong center = randomLatLong();
        double radius = 5.0;
        int numLeafTuples = 5;

        NodeHeader<LatLong> leafNode = newLeafNodeHeader(id, parent, center, radius, numLeafTuples);

        NodeOp<LatLong, String> editOperation = incrementTupleCount(leafNode);

        NodeHeader<LatLong> newLeafNode = editOperation.resultingHeader();

        assertThat(leafNode.id(), is(newLeafNode.id()));
        assertThat(leafNode.parent(), is(newLeafNode.parent()));
        assertThat(leafNode.center(), is(newLeafNode.center()));
        assertThat(leafNode.radius(), is(newLeafNode.radius()));
        assertThat(leafNode.numTuples() + 1, is(newLeafNode.numTuples()));
    }

    @Test
    void addTupleToLeaf_tupleCount_withReduce() {

        TimeId id = newId();
        TimeId parent = newId();
        LatLong center = randomLatLong();
        double radius = 5.0;
        int numLeafTuples = 5;

        NodeHeader<LatLong> leafNode = newLeafNodeHeader(id, parent, center, radius, numLeafTuples);

        NodeOp<LatLong, String> editOperation1 = incrementTupleCount(leafNode);
        NodeOp<LatLong, String> editOperation2 = incrementTupleCount(leafNode);
        NodeOp<LatLong, String> editOperation3 = incrementTupleCount(leafNode);
        NodeOp<LatLong, String> combinedOp = reduce(List.of(editOperation1, editOperation2, editOperation3));

        NodeHeader<LatLong> newLeafNode = combinedOp.resultingHeader();

        assertThat(leafNode.id(), is(newLeafNode.id()));
        assertThat(leafNode.parent(), is(newLeafNode.parent()));
        assertThat(leafNode.center(), is(newLeafNode.center()));
        assertThat(leafNode.radius(), is(newLeafNode.radius()));
        assertThat(leafNode.numTuples() + 3, is(newLeafNode.numTuples()));

        assertThrows(UnsupportedOperationException.class, () -> newLeafNode.numChildren());
    }

    @Test
    void expandRadiusOfLeaf() {

        TimeId id = newId();
        TimeId parent = newId();
        LatLong center = randomLatLong();
        double radius = 5.0;
        int numLeafTuples = 5;

        NodeHeader<LatLong> leafNode = newLeafNodeHeader(id, parent, center, radius, numLeafTuples);

        NodeOp<LatLong, String> editOperation = increaseRadiusOf(leafNode, 5.2);

        NodeHeader<LatLong> newLeafNode = editOperation.resultingHeader();

        assertThat(leafNode.id(), is(newLeafNode.id()));
        assertThat(leafNode.parent(), is(newLeafNode.parent()));
        assertThat(leafNode.center(), is(newLeafNode.center()));
        assertThat(newLeafNode.radius(), is(5.2));
        assertThat(leafNode.numTuples(), is(newLeafNode.numTuples()));

        assertThrows(UnsupportedOperationException.class, () -> newLeafNode.numChildren());
    }

    @Test
    void expandRadiusOfLeaf_withReduce() {

        TimeId id = newId();
        TimeId parent = newId();
        LatLong center = randomLatLong();
        double radius = 5.0;
        int numLeafTuples = 5;

        NodeHeader<LatLong> leafNode = newLeafNodeHeader(id, parent, center, radius, numLeafTuples);

        NodeOp<LatLong, String> editOperation1 = increaseRadiusOf(leafNode, 5.4);
        NodeOp<LatLong, String> editOperation2 = increaseRadiusOf(leafNode, 5.8);
        NodeOp<LatLong, String> editOperation3 = increaseRadiusOf(leafNode, 5.3);
        NodeOp<LatLong, String> combinedOp = reduce(List.of(editOperation1, editOperation2, editOperation3));

        NodeHeader<LatLong> newLeafNode = combinedOp.resultingHeader();

        assertThat(leafNode.id(), is(newLeafNode.id()));
        assertThat(leafNode.parent(), is(newLeafNode.parent()));
        assertThat(leafNode.center(), is(newLeafNode.center()));
        assertThat(5.8, is(newLeafNode.radius()));
        assertThat(leafNode.numTuples(), is(newLeafNode.numTuples()));

        assertThrows(UnsupportedOperationException.class, () -> newLeafNode.numChildren());
    }

    @Test
    void expandRadiusAndAddTupleToLeaf() {

        TimeId id = newId();
        TimeId parent = newId();
        LatLong center = randomLatLong();
        double radius = 5.0;
        int numLeafTuples = 5;

        NodeHeader<LatLong> leafNode = newLeafNodeHeader(id, parent, center, radius, numLeafTuples);

        NodeOp<LatLong, String> editOperation1 = incrementTupleCount(leafNode);
        NodeOp<LatLong, String> editOperation2 = increaseRadiusOf(leafNode, 5.4);
        NodeOp<LatLong, String> editOperation3 = incrementTupleCount(leafNode);
        NodeOp<LatLong, String> combinedOp = reduce(List.of(editOperation1, editOperation2, editOperation3));

        NodeHeader<LatLong> newLeafNode = combinedOp.resultingHeader();

        assertThat(leafNode.id(), is(newLeafNode.id()));
        assertThat(leafNode.parent(), is(newLeafNode.parent()));
        assertThat(leafNode.center(), is(newLeafNode.center()));
        assertThat(5.4, is(newLeafNode.radius()));
        assertThat(leafNode.numTuples() + 2, is(newLeafNode.numTuples()));

        assertThrows(UnsupportedOperationException.class, () -> newLeafNode.numChildren());
    }

    @Test
    void canMakeTupleOpForLeafNode() {

        TimeId id = newId();
        TimeId parent = newId();
        LatLong center = randomLatLong();
        double radius = 5.0;
        int numLeafTuples = 5;

        NodeHeader<LatLong> leafNode = newLeafNodeHeader(id, parent, center, radius, numLeafTuples);
        Tuple<LatLong, String> tuple = newTuple(center, "value");

        assertDoesNotThrow(() -> new TupleOp<>(leafNode, tuple));
    }

    @Test
    void cannotMakeTupleOpForInnerNode() {

        TimeId id = newId();
        TimeId parent = newId();
        LatLong center = randomLatLong();
        double radius = 5.0;

        NodeHeader<LatLong> innerNode = newInnerNodeHeader(id, parent, center, radius, List.of(newId()));
        Tuple<LatLong, String> tuple = newTuple(center, "value");

        assertThrows(IllegalArgumentException.class, () -> new TupleOp<>(innerNode, tuple));
    }
}
