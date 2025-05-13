package org.mitre.disttree;

/** A Serde is Serializer and Deserializer for a specific type T. */
public interface Serde<T> {

    byte[] toBytes(T item);

    T fromBytes(byte[] bytes);
}
