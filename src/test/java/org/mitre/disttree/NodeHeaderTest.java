package org.mitre.disttree;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mitre.caasd.commons.ids.TimeId.newId;
import static org.mitre.disttree.NodeHeader.newLeafNodeHeader;

import java.util.List;

import org.junit.jupiter.api.Test;

class NodeHeaderTest {

    @Test
    void canBuildLeafNode() {
        assertDoesNotThrow(() -> newLeafNodeHeader(newId(), newId(), "center", 12, 5));
        assertDoesNotThrow(() -> new NodeHeader<>(newId(), newId(), "center", 12, null, 5));
    }

    @Test
    void canBuildLeafWithoutChildren() {
        // necessary when we repack Leaf nodes, we need a temporary "empty node"
        assertDoesNotThrow(() -> newLeafNodeHeader(newId(), newId(), "center", 12, 0));
        assertDoesNotThrow(() -> new NodeHeader<>(newId(), newId(), "center", 12, null, 5));
    }

    @Test
    void rejectLeafsWithChildren() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new NodeHeader<>(newId(), newId(), "center", 12, List.of(newId()), 5));
    }

    @Test
    void canBuildEmptyInnerNode() {
        assertDoesNotThrow(() -> new NodeHeader<>(newId(), newId(), "center", 12, emptyList(), 0));
    }

    @Test
    void canBuildInnerNode_withChild() {
        assertDoesNotThrow(() -> new NodeHeader<>(newId(), newId(), "center", 12, List.of(newId()), 0));
    }
}
