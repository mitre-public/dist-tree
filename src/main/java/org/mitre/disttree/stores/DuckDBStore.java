package org.mitre.disttree.stores;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

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

        //        stmt.execute("CREATE TABLE IF NOT EXISTS nodes (id VARCHAR, parentId VARCHAR, base64Center VARCHAR, "
        //                + "radius DOUBLE, childNodeIds VARCHAR[], numTuples INTEGER)");
        stmt.execute(
                "CREATE TABLE IF NOT EXISTS nodes (id VARCHAR PRIMARY KEY, parentId VARCHAR, base64Center VARCHAR, "
                        + "radius DOUBLE, childNodeIds VARCHAR[], numTuples INTEGER)");
        stmt.execute(
                "CREATE TABLE IF NOT EXISTS tuples (tupleId VARCHAR, pageId VARCHAR, key VARCHAR, " + "value VARCHAR)");
        stmt.execute("CREATE TABLE IF NOT EXISTS transactions (transactionId VARCHAR, time BIGINT)");
        stmt.execute("CREATE TABLE IF NOT EXISTS roots (rootId VARCHAR, time BIGINT)");

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

            id = TimeId.fromBase64(rs.getString("transactionId"));
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

            id = TimeId.fromBase64(rs.getString("rootId"));
        } catch (Exception e) {
            System.out.println("Could not get rootId from DB");
            id = null;
        }

        stmt.close();
        return id;
    }

    /** Store lastTransactionId in DB. Including time allows use to keep a history of transactionIds
     * while also allowing use to query the latest.
     */
    private void insertLastTransactionId(TimeId id) throws Exception {
        Statement stmt = conn.createStatement();

        stmt.execute(
                "INSERT INTO transactions VALUES " + "('" + id.toString() + "', " + System.currentTimeMillis() + ")");

        stmt.close();

        this.lastTransactionId = id;
    }

    /** Store rootId in DB. Including time allows use to keep a history of rootIds
     * while also allowing use to query the latest.
     */
    private void insertRootId(TimeId id) throws Exception {
        Statement stmt = conn.createStatement();

        stmt.execute("INSERT INTO roots VALUES " + "('" + id.toString() + "', " + System.currentTimeMillis() + ")");

        stmt.close();

        this.root = id;
    }

    /** Extract all tuples from DB for a given pageId and return a DataPage object */
    private DataPage<byte[], byte[]> queryTuplesByPageId(TimeId id) throws Exception {

        DataPage<byte[], byte[]> page;

        Statement stmt = conn.createStatement();

        String query = "SELECT * FROM tuples WHERE pageId = '" + id.toString() + "'";

        try {
            ResultSet rs = stmt.executeQuery(query);

            Set<Tuple<byte[], byte[]>> tuples = new HashSet<>();

            while (rs.next()) {
                tuples.add(new Tuple<>(
                        TimeId.fromBase64(rs.getString("tupleId")),
                        BASE_64_DECODER.decode(rs.getString("key")),
                        isNull(rs.getString("value")) ? null : BASE_64_DECODER.decode(rs.getString("value"))));
            }
            page = new DataPage<>(id, tuples);
        } catch (Exception e) {
            System.out.println("Exception occurred querying tuples for DataPage: " + e);
            page = null;
        }

        stmt.close();
        return page;
    }

    /** Extract node from DB for a specified id */
    private NodeHeader<byte[]> queryNodeById(Connection conn, TimeId id) throws Exception {
        NodeHeader<byte[]> node;

        Statement stmt = conn.createStatement();

        String query = "SELECT * FROM nodes WHERE id = '" + id.toString() + "'";

        try {
            ResultSet rs = stmt.executeQuery(query);

            // Move cursor to next row of ResultSet.
            rs.next();

            // Convert sql array to object array and then to string array.
            Object[] childIds = isNull(rs.getArray("childNodeIds"))
                    ? null
                    : (Object[]) rs.getArray("childNodeIds").getArray();

            String[] childIdsList =
                    isNull(childIds) ? null : Arrays.stream(childIds).toArray(String[]::new);

            // Create node from retrieved data
            node = new NodeHeader<>(
                    TimeId.fromBase64(rs.getString("id")),
                    isNull(rs.getString("parentId")) ? null : TimeId.fromBase64(rs.getString("parentId")),
                    BASE_64_DECODER.decode(rs.getString("base64Center")),
                    rs.getDouble("radius"),
                    asTimeIdList(childIdsList),
                    rs.getInt("numTuples"));
        } catch (Exception e) {
            System.out.println("Error querying id: " + id);
            node = null;
        }

        stmt.close();
        return node;
    }

    private void batchInsertTuples(List<TupleAssignment<byte[], byte[]>> tuples) {

        String query = "INSERT INTO tuples(tupleId, pageId, key, value) VALUES (?,?,?,?)";
        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            tuples.forEach(ta -> {
                try {
                    pStmt.setString(1, ta.tuple().id().asBase64());
                    pStmt.setString(2, ta.pageId().asBase64());
                    pStmt.setString(3, BASE_64_ENCODER.encodeToString(ta.tuple().key()));
                    pStmt.setString(
                            4,
                            isNull(ta.tuple().value())
                                    ? null
                                    : BASE_64_ENCODER.encodeToString(ta.tuple().value()));
                    pStmt.addBatch();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            pStmt.executeBatch();
        } catch (Exception e) {
            System.out.println("Error batch inserting tuples");
        }
    }

    private void batchInsertNodes(List<NodeHeader<byte[]>> updatedHeaders) throws Exception {
        //        batchDeleteNodes(updatedHeaders);

        //        String query =
        //                "INSERT INTO nodes(id, parentId, base64Center, radius, childNodeIds, numTuples) VALUES
        // (?,?,?,?,?,?)";

        String query =
                "INSERT OR REPLACE INTO nodes(id, parentId, base64Center, radius, childNodeIds, numTuples) VALUES (?,?,?,?,?,?)";

        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            updatedHeaders.forEach(nodeHeader -> {
                try {
                    pStmt.setString(1, nodeHeader.id().toString());
                    pStmt.setString(
                            2,
                            isNull(nodeHeader.parent())
                                    ? null
                                    : nodeHeader.parent().toString());
                    pStmt.setBytes(3, BASE_64_ENCODER.encode(nodeHeader.center()));
                    pStmt.setDouble(4, nodeHeader.radius());
                    pStmt.setObject(5, asVarcharArray(nodeHeader));
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
    private void deleteTupleById(Connection conn, TimeId id) throws Exception {
        String query = "DELETE FROM tuples WHERE tupleId = '" + id.toString() + "'";

        Statement stmt = conn.createStatement();

        stmt.execute(query);

        stmt.close();
    }

    /** Delete all tuples from DB for a specified pageId  */
    private void deleteTuplesByPageId(TimeId pageId) throws Exception {
        String query = "DELETE FROM tuples WHERE pageId = '" + pageId.toString() + "'";

        Statement stmt = conn.createStatement();

        stmt.execute(query);

        stmt.close();
    }

    /** Delete all tuples from DB for a list of pageIds  */
    private void batchDeleteTuplesByPageId(Set<TimeId> pageIds) throws Exception {

        String query = "DELETE FROM tuples WHERE pageId = ?";

        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            pageIds.forEach(pageId -> {
                try {
                    pStmt.setString(1, pageId.toString());
                    pStmt.addBatch();

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            pStmt.executeBatch();
        }
    }

    /** Delete node from DB for a specified id */
    private void deleteNodeById(TimeId id) throws Exception {
        String query = "DELETE FROM nodes WHERE id = '" + id.toString() + "'";

        Statement stmt = conn.createStatement();

        stmt.execute(query);

        stmt.close();
    }

    /** Delete node from DB for a list of nodeHeaders */
    private void batchDeleteNodes(List<NodeHeader<byte[]>> nodeHeaders) throws Exception {

        String query = "DELETE FROM nodes WHERE id = ?";

        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            nodeHeaders.forEach(nodeHeader -> {
                try {
                    pStmt.setString(1, nodeHeader.id().toString());
                    pStmt.addBatch();

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            pStmt.executeBatch();
        }
    }

    /** Delete node from DB for a list of nodeIds */
    private void batchDeleteNodesById(Set<TimeId> nodeIds) throws Exception {

        String query = "DELETE FROM nodes WHERE id = ?";

        try (PreparedStatement pStmt = conn.prepareStatement(query)) {
            nodeIds.forEach(nodeId -> {
                try {
                    pStmt.setString(1, nodeId.toString());
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
            node = queryNodeById(this.conn, id);
        } catch (Exception e) {
            System.out.println("Exception occurred querying node: " + e);
            node = null;
        }
        return node;
    }

    @Override
    public void applyTransaction(TreeTransaction<byte[], byte[]> transaction) {

        //        System.out.println(transaction.describe());

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
        batchInsertTuples(tuples);
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

    /** Convert childNodesIds to DuckDB-friendly array object. */
    private Array asVarcharArray(NodeHeader<byte[]> node) throws Exception {
        if (isNull(node.childNodes())) {
            return null;
        }

        return conn.createArrayOf(
                "VARCHAR", node.childNodes().stream().map(id -> id.asBase64()).toArray(String[]::new));
    }

    public static List<TimeId> asTimeIdList(String[] ids) {
        if (isNull(ids)) {
            return null;
        }

        return Stream.of(ids).map(id -> TimeId.fromBase64(id)).toList();
    }
}
