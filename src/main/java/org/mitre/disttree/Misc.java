package org.mitre.disttree;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.List;

class Misc {

    /**
     * @return The last element in a list.
     *
     * @throws IndexOutOfBoundsException – if the list is empty (matches behavior of List.get(-1))
     */
    static <E> E last(List<E> list) {
        return list.get(list.size() - 1);
    }

    /** Combine two lists without altering the input lists. */
    static <E> ArrayList<E> combineLists(List<E> a, List<E> b) {

        // retain nulls, DO NOT return an emptyList
        if (isNull(a) && isNull(b)) {
            return null;
        }

        ArrayList<E> combined = new ArrayList<>();
        if (nonNull(a)) {
            combined.addAll(a);
        }
        if (nonNull(b)) {
            combined.addAll(b);
        }

        return combined;
    }
}
