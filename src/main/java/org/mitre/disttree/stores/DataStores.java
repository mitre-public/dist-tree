package org.mitre.disttree.stores;

import org.mitre.disttree.DataStore;

/**
 * Contains static factory methods for pre-existing DataStores;
 */
public class DataStores {

    /**
     * @return A DataStore that stores all Node and Tuple data in JVM memory. This DataStore is fast
     *     because it DOES NOT perform I/O or durably store the data. When using this DataStore be
     *     sure to use READ_WRITE mode in your TreeConfig.
     */
    public static DataStore inMemoryStore() {
        return new InMemoryStore();
    }

    /**
     * @return A DataStore that uses DuckDB to stores all Node and Tuple data. This DataStore is
     *     optimized for fast reads. DuckDB is a well-established DB tool that operates entirely
     *     within this JVM process. In other words, using DuckDB provides durable data storage
     *     without requiring an external DB process. This is good because it simplifies deployment.
     *     This is bad because the data we store is (1) limited to the size we can store on a single
     *     node and (2) our "single node" DataStore can only be as robust as a single node can be.
     */
    public static DataStore duckDbStore() {
        return new DuckDBStore();
    }

    /**
     * @param pathToDbFiles The directory when DuckDB files will be stored
     *
     * @return A DataStore that uses DuckDB to stores all Node and Tuple data. This DataStore is
     *     optimized for fast reads. DuckDB is a well-established DB tool that operates entirely
     *     within this JVM process. In other words, using DuckDB provides durable data storage
     *     without requiring an external DB process. This is good because it simplifies deployment.
     *     This is bad because the data we store is (1) limited to the size we can store on a single
     *     node and (2) our "single node" DataStore can only be as robust as a single node can be.
     */
    public static DataStore duckDbStore(String pathToDbFiles) {
        return new DuckDBStore(pathToDbFiles);
    }
}
