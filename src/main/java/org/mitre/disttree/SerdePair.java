package org.mitre.disttree;

import static java.util.stream.Collectors.toSet;
import static org.mitre.disttree.Serdes.serializeNode;

import java.util.List;

/**
 * A SerdePair combines the Key Serde and the Value Serde in one place.  This makes it the best
 * place to put all the "aggregate serde operations" like converting Entries, NodeHeaders,
 * DataPages, and TreeTransactions.
 *
 * @param keySerde
 * @param valueSerde
 * @param <K>
 * @param <V>
 */
public record SerdePair<K, V>(Serde<K> keySerde, Serde<V> valueSerde) {

    public static <K, V> SerdePair<K, V> pair(Serde<K> keySerde, Serde<V> valueSerde) {
        return new SerdePair<>(keySerde, valueSerde);
    }

    public Tuple<byte[], byte[]> serialize(Tuple<K, V> tuple) {
        return new Tuple<>(tuple.id(), keySerde.toBytes(tuple.key()), valueSerde.toBytes(tuple.value()));
    }

    public Tuple<K, V> deserialize(Tuple<byte[], byte[]> rawTuple) {
        return new Tuple<>(rawTuple.id(), keySerde.fromBytes(rawTuple.key()), valueSerde.fromBytes(rawTuple.value()));
    }

    public List<Tuple<byte[], byte[]>> serialize(List<Tuple<K, V>> tuples) {
        return tuples.stream().map(tuple -> serialize(tuple)).toList();
    }

    public List<Tuple<K, V>> deserialize(List<Tuple<byte[], byte[]>> tuples) {
        return tuples.stream().map(byteTuple -> deserialize(byteTuple)).toList();
    }

    public DataPage<byte[], byte[]> serializePage(DataPage<K, V> typedPage) {
        return new DataPage<>(
                typedPage.id(),
                typedPage.tuples().stream()
                        .map(typedTuple -> serialize(typedTuple))
                        .collect(toSet()));
    }

    public List<TupleAssignment<byte[], byte[]>> serializeAssignments(List<TupleAssignment<K, V>> assignments) {
        return assignments.stream()
                .map(tupleAssignment -> serializeAssignment(tupleAssignment))
                .toList();
    }

    public TupleAssignment<byte[], byte[]> serializeAssignment(TupleAssignment<K, V> ta) {
        return new TupleAssignment<>(serialize(ta.tuple()), ta.pageId());
    }

    public DataPage<K, V> deserialize(DataPage<byte[], byte[]> binary) {
        return new DataPage<>(
                binary.id(), binary.tuples().stream().map(e -> deserialize(e)).collect(toSet()));
    }

    public NodeHeader<byte[]> serializeHeader(NodeHeader<K> node) {
        return serializeNode(node, keySerde);
    }

    public List<NodeHeader<byte[]>> serializeHeaders(List<NodeHeader<K>> nodes) {
        return nodes.stream().map(hdr -> serializeHeader(hdr)).toList();
    }

    public NodeHeader<K> deserializeHeader(NodeHeader<byte[]> node) {
        return new NodeHeader<>(
                node.id(),
                node.parent(),
                keySerde.fromBytes(node.center()),
                node.radius(),
                node.childNodes(),
                node.numTuples());
    }

    public TreeTransaction<byte[], byte[]> serializeTransaction(TreeTransaction<K, V> typedTransaction) {

        return new TreeTransaction<>(
                typedTransaction.expectedTreeId(),
                serializeHeaders(typedTransaction.createdNodes()),
                serializeHeaders(typedTransaction.updatedNodes()),
                serializeAssignments(typedTransaction.createdTuples()),
                serializeAssignments(typedTransaction.updatedTuples()),
                typedTransaction.deletedLeafNodes(),
                typedTransaction.deletedNodeHeaders());
    }
}
