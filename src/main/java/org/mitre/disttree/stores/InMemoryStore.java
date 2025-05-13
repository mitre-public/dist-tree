package org.mitre.disttree.stores;

import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.mitre.caasd.commons.ids.TimeId;
import org.mitre.disttree.DataPage;
import org.mitre.disttree.DataStore;
import org.mitre.disttree.NodeHeader;
import org.mitre.disttree.TreeTransaction;
import org.mitre.disttree.Tuple;
import org.mitre.disttree.TupleAssignment;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class InMemoryStore implements DataStore {

    private TimeId lastTransactionId;

    private TimeId root;

    /** Store all the Tuples at any given PageId. */
    private final Multimap<TimeId, Tuple<byte[], byte[]>> tupleMultiMap;

    private final TreeMap<TimeId, NodeHeader<byte[]>> nodes;

    InMemoryStore() {
        this.lastTransactionId = null;
        this.root = null;
        this.tupleMultiMap = TreeMultimap.create();
        this.nodes = new TreeMap<>();
    }

    /**
     * @return The id of the last TreeTransaction that altered this tree. This id allows us to
     *     ensure TreeTransactions are "built from" the correct DistanceTree state.
     */
    @Override
    public TimeId lastTransactionId() {
        return lastTransactionId;
    }

    @Override
    public TimeId rootId() {
        return root;
    }

    @Override
    public DataPage<byte[], byte[]> dataPageAt(TimeId id) {
        DataPage<byte[], byte[]> page = DataPage.asDataPage(id, tupleMultiMap.get(id));

        if (page.isEmpty()) {

            // DO NOT return something like DataPage.emptySetAt(id) when get(id) returns a null!
            // It is better to directly return the "null" so logic errors are surfaced sooner
            return null;
        } else {
            return page;
        }
    }

    @Override
    public NodeHeader<byte[]> nodeAt(TimeId id) {
        return nodes.get(id);
    }

    @Override
    public void applyTransaction(TreeTransaction<byte[], byte[]> transaction) {

        //        System.out.println(transaction.describe());

        if (lastTransactionId != transaction.expectedTreeId()) {
            throw new IllegalStateException("Cannot apply transaction, tree state has changed");
        }

        lastTransactionId = transaction.transactionId();
        deletePages(transaction.deletedLeafNodes());
        deleteNodeHeaders(transaction.deletedNodeHeaders());

        writeTuples(transaction.createdTuples());
        writeTuples(transaction.updatedTuples());

        writeHeaders(transaction.createdNodes());
        writeHeaders(transaction.updatedNodes());
        if (transaction.hasNewRoot()) {
            this.root = transaction.newRoot();
        }
    }

    private void writeTuples(List<TupleAssignment<byte[], byte[]>> tuples) {
        tuples.forEach(ta -> tupleMultiMap.put(ta.pageId(), ta.tuple()));
    }

    private void deletePages(Set<TimeId> deletedLeafNodes) {
        deletedLeafNodes.forEach(id -> tupleMultiMap.removeAll(id));
    }

    private void deleteNodeHeaders(Set<TimeId> deletedNodeHeaders) {
        deletedNodeHeaders.forEach(id -> nodes.remove(id));
    }

    /** These NodeHeaders always overwrite whatever NodeHeader exist previously. */
    private void writeHeaders(List<NodeHeader<byte[]>> updatedHeaders) {
        updatedHeaders.forEach(nodeHeader -> nodes.put(nodeHeader.id(), nodeHeader));
    }

    /**
     * This method is not used by DistanceTree during regular operations. This method was added to
     * permit writing stronger unit tests.
     *
     * @return The TimeIds of every NodeHeader stored in this DataStore
     */
    public Set<TimeId> allNodeHeaderIds() {
        return nodes.keySet();
    }

    /**
     * This method is not used by DistanceTree during regular operations. This method was added to
     * permit writing stronger unit tests.
     *
     * @return The TimeIds of every DataPage stored in this DataStore
     */
    public Set<TimeId> allDataPageIds() {
        return tupleMultiMap.keySet();
    }
}
