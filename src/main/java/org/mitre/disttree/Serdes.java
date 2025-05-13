package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.isNull;
import static org.mitre.caasd.commons.util.DemotedException.demote;

import java.io.UnsupportedEncodingException;

import org.mitre.caasd.commons.LatLong;

/**
 * Provides pre-built Serdes for common types.
 */
public class Serdes {

    /** This Serde if for when a DistanceTree NEVER uses the values in the Key,Value Tuples. */
    public static Serde<Void> voidSerde() {
        return new VoidSerde();
    }

    /** A Serde for when ONLY null values are allowed (i.e. real data is not expected or allowed). */
    public static class VoidSerde implements Serde<Void> {

        @Override
        public byte[] toBytes(Void item) {
            checkArgument(isNull(item), "null was expected because this value");
            return null;
        }

        @Override
        public Void fromBytes(byte[] bytes) {
            checkArgument(isNull(bytes), "null was expected because this value");
            return null;
        }
    }

    /** A Serde for LatLongs (does not allow null LatLongs). */
    public static Serde<LatLong> latLongSerde() {
        return new LatLongSerde();
    }

    public static class LatLongSerde implements Serde<LatLong> {

        @Override
        public byte[] toBytes(LatLong item) {
            return item.toBytes();
        }

        @Override
        public LatLong fromBytes(byte[] bytes) {
            return LatLong.fromBytes(bytes);
        }
    }

    /** A Serde for String encoded in UTF8. Null strings are permitted */
    public static Serde<String> stringUtf8Serde() {
        return new StringUtf8Serde();
    }

    public static class StringUtf8Serde implements Serde<String> {

        @Override
        public byte[] toBytes(String item) {
            return isNull(item) ? null : item.getBytes();
        }

        @Override
        public String fromBytes(byte[] bytes) {
            String ENCODING = "UTF8";
            try {
                return isNull(bytes) ? null : new String(bytes, ENCODING);
            } catch (UnsupportedEncodingException e) {
                throw demote("Error when deserializing byte[] to string due to unsupported encoding " + ENCODING, e);
            }
        }
    }

    public static <K> NodeHeader<byte[]> serializeNode(NodeHeader<K> node, Serde<K> keySerde) {
        return new NodeHeader<>(
                node.id(),
                node.parent(),
                keySerde.toBytes(node.center()),
                node.radius(),
                node.childNodes(),
                node.numTuples());
    }
}
