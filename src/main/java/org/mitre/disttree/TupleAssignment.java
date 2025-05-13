package org.mitre.disttree;

import static java.util.Objects.requireNonNull;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * A TupleAssignment corresponds to either a CREATE operation where a Tuple is added to a
 * DistanceTree for the first time OR an UPDATE operation where a Tuple is moved from one DataPage
 * to another.
 */
public record TupleAssignment<K, V>(Tuple<K, V> tuple, TimeId pageId) {

    public TupleAssignment {
        requireNonNull(tuple);
        requireNonNull(pageId);
    }

    public static <K, V> TupleAssignment<K, V> assign(Tuple<K, V> tuple, TimeId pageId) {
        return new TupleAssignment<>(tuple, pageId);
    }

    public TimeId tupleId() {
        return tuple.id();
    }

    public boolean hasPageId(TimeId id) {
        return pageId.equals(id);
    }
}
