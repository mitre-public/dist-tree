package org.mitre.disttree.stores;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import java.io.*;
import java.sql.*;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.duckdb.DuckDBConnection;
import org.mitre.caasd.commons.ids.TimeId;
import org.mitre.disttree.DataPage;
import org.mitre.disttree.DataStore;
import org.mitre.disttree.NodeHeader;
import org.mitre.disttree.TreeTransaction;
import org.mitre.disttree.Tuple;
import org.mitre.disttree.TupleAssignment;

public class DuckDBStore implements DataStore {

    private static final String DEFAULT_DB_PATH = "DuckDBStore";

    private static final Base64.Encoder BASE_64_ENCODER = Base64.getEncoder().withoutPadding();
    private static final Base64.Decoder BASE_64_DECODER = Base64.getDecoder();

    private TimeId lastTransactionId;
    private TimeId root;
    private Connection conn;
    private final String pathToDbFile;

    DuckDBStore() {
        this(DEFAULT_DB_PATH);
    }

    DuckDBStore(String pathToDbFiles) {
        requireNonNull(pathToDbFiles);

        prepareDbFolder(pathToDbFiles);

        this.pathToDbFile = pathToDbFiles;

        try {
            this.conn = getConnection();

            // Create nodes and tuples tables if they do not already exist
            createTables();

            // Get lastTransactionId and rootId from DB
            this.lastTransactionId = queryLastTransactionId();
            this.root = queryRootId();
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e);
        }
    }

    /** Ensure the DB path exists. */
    private void prepareDbFolder(String pathToDbFiles) {
        File dir = new File(pathToDbFiles);

        dir.mkdirs();

        checkState(dir.exists(), "Problem creating: " + pathToDbFiles);
        checkArgument(dir.isDirectory(), pathToDbFiles + " is not a directory");
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

    /** Create connection using supplied path else use default. */
    private Connection getConnection() throws SQLException {

        String connectionString = (isNull(pathToDbFile))
                ? "jdbc:duckdb:./disttree_database.db"
                : "jdbc:duckdb:" + this.pathToDbFile + "/disttree_database.db";

        return DriverManager.getConnection(connectionString);
    }

    /** Create tables to store tuples and node objects */
    private void createTables() throws Exception {
        Statement stmt = conn.createStatement();

        stmt.execute(
                "CREATE TABLE IF NOT EXISTS nodes (id BLOB PRIMARY KEY, parentId BLOB, base64Center BLOB, "
                        + "radius DOUBLE, childNodeIds BLOB, numTuples INTEGER)");
        stmt.execute(
                "CREATE TABLE IF NOT EXISTS tuples (tupleId BLOB, pageId BLOB, key BLOB, value BLOB)");
        stmt.execute("CREATE TABLE IF NOT EXISTS transactions (transactionId BLOB, time BIGINT)");
        stmt.execute("CREATE TABLE IF NOT EXISTS roots (rootId BLOB, time BIGINT)");

        stmt.close();
    }

    /** Attempt to get lastTransactionId from DB. Return null if empty. */
    private TimeId queryLastTransactionId() throws Exception {
        TimeId id;

        Statement stmt = conn.createStatement();

        String query = "SELECT * FROM transactions ORDER BY time DESC LIMIT 1";

        try {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();

            id = new TimeId(getBlobBytes(rs.getBlob("transactionId")));

        } catch (Exception e) {
            System.out.println("Could not get lastTransactionId from DB");
            id = null;
        }

        stmt.close();
        return id;
    }

    /** Attempt to get rootId from DB. Return null if empty. */
    private TimeId queryRootId() throws Exception {
        TimeId id;

        Statement stmt = conn.createStatement();

        String query = "SELECT * FROM roots ORDER BY time DESC LIMIT 1";

        try {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();

            id = new TimeId(getBlobBytes(rs.getBlob("rootId")));
        } catch (Exception e) {
            System.out.println("Could not get rootId from DB");
            id = null;
        }

        stmt.close();
        return id;
    }

    /** Store lastTransactionId in DB. Including time allows us to keep a history of transactionIds
     * while also allowing user to query the latest.
     */
    private void insertLastTransactionId(TimeId id) throws Exception {

        String query = "INSERT INTO transactions(transactionId, time) VALUES (?,?)";

        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            pStmt.setBytes(1, id.bytes());
            pStmt.setLong(2, System.currentTimeMillis());
            pStmt.addBatch();
            pStmt.executeBatch();

            this.lastTransactionId = id;

        } catch (Exception e) {
            System.out.println("Error inserting transactionId: " + id.toString());
        }
    }

    /** Store rootId in DB. Including time allows use to keep a history of rootIds
     * while also allowing use to query the latest.
     */
    private void insertRootId(TimeId id) throws Exception {

        String query = "INSERT INTO roots(rootId, time) VALUES (?,?)";

        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            pStmt.setBytes(1, id.bytes());
            pStmt.setLong(2, System.currentTimeMillis());
            pStmt.addBatch();
            pStmt.executeBatch();

            this.root = id;

        } catch (Exception e) {
            System.out.println("Error inserting rootId: " + id.toString());
        }
    }

    /** Extract all tuples from DB for a given pageId and return a DataPage object */
    private DataPage<byte[], byte[]> queryTuplesByPageId(TimeId id) throws Exception {

        DataPage<byte[], byte[]> page;

        PreparedStatement statement = conn.prepareStatement("SELECT * FROM tuples WHERE pageId = ?");
        statement.setBytes(1, id.bytes());


        try {
            ResultSet rs = statement.executeQuery();

            Set<Tuple<byte[], byte[]>> tuples = new HashSet<>();

            while (rs.next()) {
                tuples.add(new Tuple<>(
                        new TimeId(getBlobBytes(rs.getBlob("tupleId"))),
                        getBlobBytes(rs.getBlob("key")),
                        isNull(rs.getBlob("value"))
                                ? null
                                : getBlobBytes(rs.getBlob("value"))
                ));
            }
            page = new DataPage<>(id, tuples);

        } catch (Exception e) {
            System.out.println("Exception occurred querying tuples for DataPage: " + e);
            page = null;
        }

        statement.close();
        return page;
    }

    /** Get bytes from blob object returned by result set */
    private static byte[] getBlobBytes(Blob blob) throws Exception {

        return blob.getBytes(1, (int) blob.length());
    }

    /** Extract node from DB for a specified id */
    private NodeHeader<byte[]> queryNodeById(TimeId id) throws Exception {
        NodeHeader<byte[]> node;

        PreparedStatement statement = conn.prepareStatement("SELECT * FROM nodes WHERE id = ?");
        statement.setBytes(1, id.bytes());

        try {
            ResultSet rs = statement.executeQuery();

            // Move cursor to next row of ResultSet.
            rs.next();

            Object childIds = isNull(rs.getBlob("childNodeIds"))
                    ? null
                    : deserialize(rs.getBlob("childNodeIds").getBinaryStream());

            List<TimeId> childIdsList =
                    isNull(childIds) ? null : (List<TimeId>) childIds;

            // Create node from retrieved data
            node = new NodeHeader<>(
                    new TimeId(getBlobBytes(rs.getBlob("id"))),
                    isNull(rs.getBlob("parentId"))
                            ? null
                            : new TimeId(getBlobBytes(rs.getBlob("parentId"))),
                    getBlobBytes(rs.getBlob("base64Center")),
                    rs.getDouble("radius"),
                    childIdsList,
                    rs.getInt("numTuples"));
        } catch (Exception e) {
            System.out.println("Error querying id: " + id);
            System.out.println(e);

            node = null;
        }

        statement.close();
        return node;
    }

//    private void batchInsertTuples(List<TupleAssignment<byte[], byte[]>> tuples) {
//
//        String query = "INSERT INTO tuples(tupleId, pageId, key, value) VALUES (?,?,?,?)";
//        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
//            tuples.forEach(ta -> {
//                try {
//                    pStmt.setString(1, ta.tuple().id().asBase64());
//                    pStmt.setString(2, ta.pageId().asBase64());
//                    pStmt.setString(3, BASE_64_ENCODER.encodeToString(ta.tuple().key()));
//                    pStmt.setString(
//                            4,
//                            isNull(ta.tuple().value())
//                                    ? null
//                                    : BASE_64_ENCODER.encodeToString(ta.tuple().value()));
//                    pStmt.addBatch();
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            pStmt.executeBatch();
//        } catch (Exception e) {
//            System.out.println("Error batch inserting tuples");
//        }
//    }

    /** Use appender instead of prepared statements when inserting many records at once */
    private void appenderInsertTuples(List<TupleAssignment<byte[], byte[]>> tuples) {

        DuckDBConnection duckConn = (DuckDBConnection) conn;
        try (var appender = duckConn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "tuples")) {
            tuples.forEach(ta -> {
                try {
                    appender.beginRow();
                    appender.append(ta.tuple().id().bytes());
                    appender.append(ta.pageId().bytes());
                    appender.append(ta.tuple().key());
                    appender.append(
                            isNull(ta.tuple().value())
                                    ? null
                                    : ta.tuple().value());
                    appender.endRow();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            System.out.println("Error batch inserting tuples");
        }
    }

    /** Serialize object to bytes for writing to DB as a blob */
    private static byte[] serialize(final Object obj) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
//            BASE_64_ENCODER.wrap(bos)
            out.writeObject(obj);
            out.flush();
            return bos.toByteArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

//    static Object deserialize(byte[] bytes) {
//        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
//
//        try (ObjectInput in = new ObjectInputStream(bis)) {
//            return in.readObject();
//        } catch (Exception ex) {
//            throw new RuntimeException(ex);
//        }
//    }

    /** Deserialize inputstream of bytes (from blob) to object */
    private static Object deserialize(InputStream stream) throws Exception {

        ObjectInputStream ois = new ObjectInputStream(stream);
        try {
            return ois.readObject();
        } finally {
            ois.close();
        }
    }

    /** Insert nodes in batched to speed things up */
    private void batchInsertNodes(List<NodeHeader<byte[]>> updatedHeaders) throws Exception {

        String query =
                "INSERT OR REPLACE INTO nodes(id, parentId, base64Center, radius, childNodeIds, numTuples) VALUES (?,?,?,?,?,?)";

        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            updatedHeaders.forEach(nodeHeader -> {
                try {
                    pStmt.setBytes(1, nodeHeader.id().bytes());
                    pStmt.setBytes(
                            2,
                            isNull(nodeHeader.parent())
                                    ? null
                                    : nodeHeader.parent().bytes());
                    pStmt.setBytes(3, nodeHeader.center());
                    pStmt.setDouble(4, nodeHeader.radius());
                    pStmt.setBytes(5, serialize(nodeHeader.childNodes()));
                    pStmt.setInt(6, nodeHeader.numTuples());
                    pStmt.addBatch();

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            pStmt.executeBatch();
        } catch (Exception e) {
            System.out.println("Error inserting nodes for batch");
        }
    }

    /** Delete tuple from DB for a specified id */
//    private void deleteTupleById(TimeId id) throws Exception {
//        String query = "DELETE FROM tuples WHERE tupleId = '" + id.toString() + "'";
//
//        Statement stmt = conn.createStatement();
//
//        stmt.execute(query);
//
//        stmt.close();
//    }

    /** Delete all tuples from DB for a specified pageId  */
//    private void deleteTuplesByPageId(TimeId pageId) throws Exception {
//        String query = "DELETE FROM tuples WHERE pageId = '" + pageId.toString() + "'";
//
//        Statement stmt = conn.createStatement();
//
//        stmt.execute(query);
//
//        stmt.close();
//    }

    /** Delete all tuples from DB for a list of pageIds  */
    private void batchDeleteTuplesByPageId(Set<TimeId> pageIds) throws Exception {

        try (PreparedStatement pStmt = conn.prepareStatement("DELETE FROM tuples WHERE pageId = ?")) {
            pageIds.forEach(pageId -> {
                try {
                    pStmt.setBytes(1, pageId.bytes());
                    pStmt.addBatch();

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            pStmt.executeBatch();
        }
    }

    /** Delete node from DB for a specified id */
//    private void deleteNodeById(TimeId id) throws Exception {
//        String query = "DELETE FROM nodes WHERE id = '" + id.toString() + "'";
//
//        Statement stmt = conn.createStatement();
//
//        stmt.execute(query);
//
//        stmt.close();
//    }

    /** Delete node from DB for a list of nodeHeaders */
//    private void batchDeleteNodes(List<NodeHeader<byte[]>> nodeHeaders) throws Exception {
//
//        String query = "DELETE FROM nodes WHERE id = ?";
//
//        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
//            nodeHeaders.forEach(nodeHeader -> {
//                try {
//                    pStmt.setString(1, nodeHeader.id().toString());
//                    pStmt.addBatch();
//
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//
//            pStmt.executeBatch();
//        }
//    }

    /** Delete node from DB for a list of nodeIds */
    private void batchDeleteNodesById(Set<TimeId> nodeIds) throws Exception {

        String query = "DELETE FROM nodes WHERE id = ?";

        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            nodeIds.forEach(nodeId -> {
                try {
                    pStmt.setBytes(1, nodeId.bytes());
                    pStmt.addBatch();

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            pStmt.executeBatch();
        }
    }

    @Override
    public DataPage<byte[], byte[]> dataPageAt(TimeId id) {

        DataPage<byte[], byte[]> page;

        try {
            page = queryTuplesByPageId(id);
        } catch (Exception e) {
            System.out.println("Exception occurred querying dataPage: " + e);
            page = null;
        }

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
        NodeHeader<byte[]> node;

        try {
            node = queryNodeById(id);
        } catch (Exception e) {
            System.out.println("Exception occurred querying node: " + e);
            node = null;
        }
        return node;
    }

    @Override
    public void applyTransaction(TreeTransaction<byte[], byte[]> transaction) {

//                System.out.println(transaction.describe());

        if (lastTransactionId != transaction.expectedTreeId()) {
            throw new IllegalStateException("Cannot apply transaction, tree state has changed");
        }

        // todo: need to make sure either all transactions succeed or all fail before committing to DB
        try {
            insertLastTransactionId(transaction.transactionId());
            deletePages(transaction.deletedLeafNodes());
            deleteNodeHeaders(transaction.deletedNodeHeaders());

            writeTuples(transaction.createdTuples());
            writeTuples(transaction.updatedTuples());

            writeHeaders(transaction.createdNodes());
            writeHeaders(transaction.updatedNodes());
            if (transaction.hasNewRoot()) {
                insertRootId(transaction.newRoot());
            }

        } catch (Exception e) {
            System.out.println("Cannot apply transaction. Exception occurred: " + e);
        }
    }

    private void writeTuples(List<TupleAssignment<byte[], byte[]>> tuples) {
        appenderInsertTuples(tuples);
    }

    private void deletePages(Set<TimeId> deletedLeafNodes) {

        try {
            batchDeleteTuplesByPageId(deletedLeafNodes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteNodeHeaders(Set<TimeId> deletedNodeHeaders) {

        try {
            batchDeleteNodesById(deletedNodeHeaders);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** These NodeHeaders always overwrite whatever NodeHeader exist previously. */
    private void writeHeaders(List<NodeHeader<byte[]>> updatedHeaders) {

        try {
            batchInsertNodes(updatedHeaders);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    /** Convert childNodesIds to DuckDB-friendly array object. */
//    private Array asVarcharArray(NodeHeader<byte[]> node) throws Exception {
//        if (isNull(node.childNodes())) {
//            return null;
//        }
//
//        return conn.createArrayOf(
//                "VARCHAR", node.childNodes().stream().map(id -> id.asBase64()).toArray(String[]::new));
//    }
//
//    public static List<TimeId> asTimeIdList(String[] ids) {
//        if (isNull(ids)) {
//            return null;
//        }
//
//        return Stream.of(ids).map(id -> TimeId.fromBase64(id)).toList();
//    }
}
