package org.mitre.disttree;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.mitre.disttree.CountingDistanceMetric.instrument;
import static org.mitre.disttree.TreeConfig.ReadWriteMode.*;
import static org.mitre.disttree.TreeConfig.RepackingMode.INCREMENTAL_LN;
import static org.mitre.disttree.TreeConfig.RepackingMode.NONE;

import org.mitre.disttree.stores.DataStores;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeConfig<K, V> {

    /**
     * Controls how many DataPages are repacked as the tree is built.  NONE = No repacking is
     * done, this is usually best when the DistanceMetric is expensive to calculate.  INCREMENTAL_LN
     * = Each batch will repack n DataPages where n = Log(number of leaf nodes).  This is usually
     * best if the tree being built will require many reads.
     */
    public enum RepackingMode {
        NONE,
        INCREMENTAL_LN
    }

    /**
     * READ_ONLY = Accept Search Queries, Reject Batches, WRITE_ONLY = Reject Search Queries, Accept
     * Batches, READ_WRITE = Accept Search Queries adn Accept Batches.
     */
    public enum ReadWriteMode {
        READ_ONLY,
        WRITE_ONLY,
        READ_AND_WRITE
    }

    static final Logger LOGGER = LoggerFactory.getLogger(TreeConfig.class);

    /** The maximum number of childNodes each RoutingNode may have. */
    final int branchingFactor;

    /** The maximum number of Tuples each DataPage (i.e., leaf node) may have. */
    final int maxTuplesPerPage;

    /** An instrumented DistanceMetric that measures the space between Tuple keys. */
    final CountingDistanceMetric<K> distMetric;

    /** The component that stores raw binary data. */
    final DataStore dataStore;

    /** A Serde for Tuple keys. */
    final Serde<K> keySerde;

    /** A Serde for Tuple values. */
    final Serde<V> valueSerde;

    final SerdePair<K, V> serde;

    final RepackingMode repackingMode;

    final ReadWriteMode readWriteMode;

    public TreeConfig() {
        this(builder());
    }

    private TreeConfig(Builder<K, V> builder) {
        this.branchingFactor = builder.branchingFactor;
        this.maxTuplesPerPage = builder.maxTuplesPerPage;
        this.distMetric = instrument(builder.distMetric);
        this.dataStore = builder.dataStore;
        this.keySerde = builder.keySerde;
        this.valueSerde = builder.valueSerde;
        this.serde = new SerdePair<>(keySerde, valueSerde);
        this.repackingMode = builder.repackingMode;
        this.readWriteMode = builder.readWriteMode;

        LOGGER.atInfo()
                .setMessage("TreeConfig.branchingFactor: {}")
                .addArgument(branchingFactor)
                .log();
        LOGGER.atInfo()
                .setMessage("TreeConfig.maxTuplesPerPage: {}")
                .addArgument(maxTuplesPerPage)
                .log();

        LOGGER.atInfo()
                .setMessage("TreeConfig.repackingMode: {}")
                .addArgument(repackingMode)
                .log();
        LOGGER.atInfo()
                .setMessage("TreeConfig.readWriteMode: {}")
                .addArgument(readWriteMode)
                .log();
        LOGGER.atInfo()
                .setMessage("TreeConfig.distMetric: {}")
                .addArgument(distMetric.innerMetric().getClass().getSimpleName())
                .log();
        LOGGER.atInfo()
                .setMessage("TreeConfig.dataStore: {}")
                .addArgument(dataStore.getClass().getSimpleName())
                .log();
        LOGGER.atInfo()
                .setMessage("TreeConfig.keySerde: {}")
                .addArgument(keySerde.getClass().getSimpleName())
                .log();
        LOGGER.atInfo()
                .setMessage("TreeConfig.valueSerde: {}")
                .addArgument(valueSerde.getClass().getSimpleName())
                .log();
    }

    public int branchingFactor() {
        return branchingFactor;
    }

    public int maxTuplesPerPage() {
        return maxTuplesPerPage;
    }

    public SerdePair<K, V> serdePair() {
        return serde;
    }

    /** @return A CountingDistanceMetric that wraps the DistanceMetric provided at construction. */
    public CountingDistanceMetric<K> distMetric() {
        return distMetric;
    }

    public static <A, B> Builder<A, B> builder() {
        return new Builder<>();
    }

    public static class Builder<K, V> {
        int branchingFactor = 64;
        int maxTuplesPerPage = 50;
        RepackingMode repackingMode = INCREMENTAL_LN;
        ReadWriteMode readWriteMode = READ_AND_WRITE;
        DataStore dataStore = null; // A default DuckDbStore is loaded at "build()" if this is null

        DistanceMetric<K> distMetric;
        Serde<K> keySerde;
        Serde<V> valueSerde;

        public Builder<K, V> branchingFactor(int branchingFactor) {
            checkArgument(branchingFactor >= 2);
            this.branchingFactor = branchingFactor;
            return this;
        }

        public Builder<K, V> maxTuplesPerPage(int maxTuplesPerPage) {
            checkArgument(maxTuplesPerPage >= 5);
            this.maxTuplesPerPage = maxTuplesPerPage;
            return this;
        }

        public Builder<K, V> distMetric(DistanceMetric<K> distMetric) {
            requireNonNull(distMetric);
            this.distMetric = distMetric;
            return this;
        }

        public Builder<K, V> dataStore(DataStore dataStore) {
            requireNonNull(dataStore);
            this.dataStore = dataStore;
            return this;
        }

        public Builder<K, V> keySerde(Serde<K> keySerde) {
            this.keySerde = requireNonNull(keySerde);
            return this;
        }

        public Builder<K, V> valueSerde(Serde<V> valueSerde) {
            this.valueSerde = requireNonNull(valueSerde);
            return this;
        }

        /** When the Tree is being built do not repack DataPages as you go. */
        public Builder<K, V> noRepacking() {
            return repackingMode(NONE);
        }

        /** When the Tree is being built repack ln(numLeafNodes) DataPages per batch. */
        public Builder<K, V> incrementalRepacking() {
            return repackingMode(INCREMENTAL_LN);
        }

        public Builder<K, V> repackingMode(RepackingMode mode) {
            requireNonNull(mode);
            this.repackingMode = mode;
            return this;
        }

        /** The resulting Tree can only process Search Queries (no new data). */
        public Builder<K, V> readOnly() {
            return readWriteMode(READ_ONLY);
        }

        /** The resulting Tree can only add Batches of data (no Search Queries). */
        public Builder<K, V> writeOnly() {
            return readWriteMode(WRITE_ONLY);
        }

        /** The resulting Tree can both process Search Queries and add Batches of data. */
        public Builder<K, V> readAndWrite() {
            return readWriteMode(READ_AND_WRITE);
        }

        public Builder<K, V> readWriteMode(ReadWriteMode mode) {
            requireNonNull(mode);
            this.readWriteMode = mode;
            return this;
        }

        public TreeConfig<K, V> build() {
            requireNonNull(distMetric, "The distMetric was not specified");
            requireNonNull(keySerde, "The keySerde was not specified");
            requireNonNull(valueSerde, "The valueSerde was not specified");

            if (isNull(dataStore)) {
                dataStore = DataStores.duckDbStore();
            }

            return new TreeConfig<>(this);
        }

        /** Equivalent to "new DistanceTree(this.build());" */
        public DistanceTree<K, V> buildTree() {
            return new DistanceTree<>(build());
        }
    }
}
