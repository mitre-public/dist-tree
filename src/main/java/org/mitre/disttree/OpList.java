package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.groupingBy;
import static org.mitre.disttree.Ops.NodeOp.reduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.mitre.caasd.commons.ids.TimeId;
import org.mitre.disttree.Ops.NodeOp;
import org.mitre.disttree.Ops.TreeOperation;

/**
 * An OpList is a List of TreeOperations that we gradually reduce until it is trivial to convert it
 * to a TreeTransaction.
 */
public class OpList<K, V> {

    /** The Working set of TreeOperations . */
    private final List<TreeOperation<K, V>> treeOps;

    /**
     * @param rawOps A Collection of various TreeOperations including: CreateRoot, UpdateNode, and
     *               CreateTuple ops.
     */
    OpList(List<TreeOperation<K, V>> rawOps) {
        this.treeOps = new ArrayList<>(rawOps);
    }

    boolean isSeedingTreeForFirstTime() {
        // When there is no root node EVERY operation will want to build the root...
        return treeOps.stream().anyMatch(op -> op instanceof Ops.CreateRoot<K, V>);
    }

    List<Tuple<K, V>> extractSeedTuples() {

        checkState(isSeedingTreeForFirstTime());

        // When there is no root node EVERY operation will want to build the root...
        List<Ops.CreateRoot<K, V>> rootOps = treeOps.stream()
                .filter(op -> op instanceof Ops.CreateRoot<K, V>)
                .map(op -> (Ops.CreateRoot<K, V>) op)
                .toList();

        // Verify all TreeOperation are, in fact, instances of CreateRoot
        checkState(rootOps.size() == treeOps.size());

        // Isolate all the "wanna-bes root Tuples".
        return rootOps.stream().map(createRoot -> createRoot.firstTuple()).toList();
    }

    public List<NodeHeader<K>> resultingHeaders() {
        List<NodeOp<K, V>> nodeOps = justNodeOps(treeOps);
        List<NodeOp<K, V>> compactedNodeOps = compactNodeOps(nodeOps);
        return applyOpsToHeaders(compactedNodeOps);
    }

    public List<TupleAssignment<K, V>> tupleAssignments() {
        return asAssignments(justCreateTupleOps(treeOps));
    }

    /**
     * Reduce a raw uncompacted List of UpdateNodeOp down to exactly one Ops for each NodeHeader
     * that needs to change
     */
    public static <K, V> List<NodeOp<K, V>> compactNodeOps(List<NodeOp<K, V>> radiusOps) {

        // Find all the NodeEditOps that act on the same Node
        Map<TimeId, List<NodeOp<K, V>>> opsByRoute =
                radiusOps.stream().collect(groupingBy(op -> op.node().id()));

        // For each Route, find the largest IncreaseRadiusOp
        return opsByRoute.values().stream().map(list -> reduce(list)).toList();
    }

    // Filter a list of TreeOperations down to just the NodeOps
    public static <K, V> List<NodeOp<K, V>> justNodeOps(List<TreeOperation<K, V>> treeOps) {
        return treeOps.stream()
                .filter(op -> op instanceof NodeOp<K, V>)
                .map(op -> (NodeOp<K, V>) op)
                .toList();
    }

    /** Apply these NodeOp actions to generate a list of "resulting NodeHeaders". */
    public List<NodeHeader<K>> applyOpsToHeaders(List<NodeOp<K, V>> nodeOps) {
        return nodeOps.stream().map(op -> op.resultingHeader()).collect(Collectors.toList()); // is mutable!
    }

    /** Convert these CreateTuple ops to TupleAssignments. */
    public static <K, V> List<TupleAssignment<K, V>> asAssignments(List<Ops.TupleOp<K, V>> tupleOps) {
        return tupleOps.stream()
                .map(op -> new TupleAssignment<>(op.tuple(), op.pageId()))
                .toList();
    }

    // Filter a list of TreeOperations down to just the TupleOps
    public static <K, V> List<Ops.TupleOp<K, V>> justCreateTupleOps(List<TreeOperation<K, V>> treeOps) {
        return treeOps.stream()
                .filter(op -> op instanceof Ops.TupleOp<K, V>)
                .map(op -> (Ops.TupleOp<K, V>) op)
                .toList();
    }
}
