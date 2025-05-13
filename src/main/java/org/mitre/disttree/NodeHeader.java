package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.*;
import static org.mitre.disttree.Misc.combineLists;

import java.util.ArrayList;
import java.util.List;

import org.mitre.caasd.commons.ids.TimeId;

/**
 * A NodeHeader summarizes a Node in a DistanceTree.  NodeHeader's are intended to be "small to
 * store" so they can be aggressively cached.  The cache-friendly nature of NodeHeader's helps us
 * find the ID of DataPages that need to be loaded without requiring IO Ops
 *
 * @param id         A unique ID that identifies this particular node.
 * @param parent     A unique ID that identifies the parent of this node.
 * @param center     The Key at the "center" of this node
 * @param radius     The current radius of this node
 * @param childNodes When leaf node = null, When inner node = Lists the ids of all child nodes
 * @param numTuples  When leaf node = n, When inner node = 0 (and failure to access)
 * @param <K>        The Keys defining the Metric Space
 */
public record NodeHeader<K>(TimeId id, TimeId parent, K center, double radius, List<TimeId> childNodes, int numTuples) {

    public NodeHeader {
        requireNonNull(id);
        requireNonNull(center);
        checkArgument(radius >= 0);
        boolean hasChildNodes = nonNull(childNodes);
        boolean hasData = numTuples > 0;

        checkArgument(!(hasChildNodes && hasData), "Cannot have both child nodes AND tuples");
    }

    /** @return A new NodeHeader that corresponds to a leaf node (the DataPage is made separately). */
    public static <K> NodeHeader<K> newLeafNodeHeader(
            TimeId id, TimeId parent, K center, double radius, int numDataEntries) {

        return new NodeHeader<>(id, parent, center, radius, null, numDataEntries);
    }

    /**
     * Used when we repack a Node from scratch. The resulting node cannot "carry over" old radius
     * info or old numTuple data.
     */
    public NodeHeader<K> zeroRadiusZeroTupleCopy() {
        checkState(this.isLeafNode());
        return new NodeHeader<>(id, parent, center, 0.0, childNodes, 0);
    }

    /**
     * @return A copy of this NodeHeader with one less child.  The radius collapses to 0 when the
     *     last child node is removed.
     */
    public NodeHeader<K> removeChild(TimeId deletedChild) {
        if (isLeafNode()) {
            throw new UnsupportedOperationException("Leaf node's have no children to remove");
        }
        if (!childNodes.contains(deletedChild)) {
            throw new UnsupportedOperationException("Inner node attempting to remove child it does not have");
            //            throw new UnsupportedOperationException("Inner node attempting to remove child it does not
            // have\n  parent: " + this + "missing child: " + deletedChild);
        }

        List<TimeId> updatedChildren = new ArrayList<>(childNodes);
        updatedChildren.remove(deletedChild);

        double updatedRadius = updatedChildren.isEmpty() ? 0 : radius;

        return newInnerNodeHeader(id, parent, center, updatedRadius, updatedChildren);
    }

    /** @return A copy of the NodeHeader but with one child replaced. */
    public NodeHeader<K> replaceChild(TimeId deletedChild, TimeId newChild) {
        if (isLeafNode()) {
            throw new UnsupportedOperationException("Leaf node's have no children to replace");
        }
        if (!childNodes.contains(deletedChild)) {
            throw new UnsupportedOperationException("Attempting to remove child it does not have");
        }
        if (childNodes.contains(newChild)) {
            throw new UnsupportedOperationException("Cannot add child that already exists");
        }

        List<TimeId> updatedChildren = new ArrayList<>(childNodes);
        updatedChildren.remove(deletedChild);
        updatedChildren.add(newChild);

        return newInnerNodeHeader(id, parent, center, radius, updatedChildren);
    }

    /** @return A new NodeHeader corresponding to an InnerNode with a List of childNodes. */
    public static <K> NodeHeader<K> newInnerNodeHeader(
            TimeId id, TimeId parent, K center, double radius, List<TimeId> childNodes) {
        requireNonNull(childNodes);
        // childNodes.size() can be 0, we need to support temporary inner nodes without children
        // (they get deleted before being committed to the tree)
        return new NodeHeader<>(id, parent, center, radius, childNodes, 0);
    }

    /** @return A copy of this NodeHeader that has a new parent. */
    public NodeHeader<K> withParent(TimeId newParent) {
        return new NodeHeader<>(id, newParent, center, radius, childNodes, numTuples);
    }

    /** @return A copy of this NodeHeader with one more child node. */
    public NodeHeader<K> addChild(TimeId childId) {
        return newInnerNodeHeader(id, parent, center, radius, combineLists(childNodes, List.of(childId)));
    }

    /**
     * @return When a Node corresponds to a leaf node return {@code numTuples > maxEntries}, else
     *     return {@code numChildren() > maxChildNodes}.
     */
    boolean isSplittable(int maxChildNodes, int maxEntries) {
        return isLeafNode() ? numTuples > maxEntries : numChildren() > maxChildNodes;
    }

    int numChildren() {

        // Fails when called on a leaf node to help track down logic errors
        if (isLeafNode()) {
            throw new UnsupportedOperationException("Leaf nodes have no children");
        }

        return childNodes.size();
    }

    boolean isLeafNode() {
        return !isInnerNode();
    }

    boolean isInnerNode() {
        return nonNull(childNodes);
    }

    /** Any NodeHeader whose parent is null is a root node. */
    boolean isRoot() {
        return isNull(parent);
    }

    /** Shorthand for id().equals(id). */
    public boolean hasId(TimeId timeId) {
        return id.equals(timeId);
    }
}
