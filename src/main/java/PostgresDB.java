import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import utils.QueryHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;
import java.util.Vector;

public class PostgresDB {
    private Connection connection;
    private String url;
    private Properties props;

    private Statement lastStatement;
    private static Long chunkSize = 0L;
    private static String snapshotID;

    private int fetchSize;

    private int numberOfThreads;

    private static final Logger LOG = LogManager.getLogger(PostgresDB.class.getName());

    PostgresDB(String url, Properties props) throws SQLException, IOException{
        this.url = url;
        this.props = props;
        this.connection = initiateConnection(url, props);

//        fetchSize = Integer.parseInt(UserProperties.getProps().getProperty("fetchSize","5000"));
        fetchSize = 5000;
        numberOfThreads = 4;
    }

    private Connection initiateConnection(String url, Properties props) throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = DriverManager.getConnection(url, props);
        connection.setAutoCommit(false);
        return connection;
    }

    public Connection getConnection(){
        return this.connection;
    }

    // Overloading for Default Parameters
    public Vector<String> listTables() throws SQLException {
        return listTables("public");
    }
    public Vector<String> listTables(String schema) throws SQLException {
        Vector<String> tables = new Vector<String>();
        PreparedStatement statement = this.connection.prepareStatement("" +
                "SELECT table_name " +
                "FROM INFORMATION_SCHEMA.tables " +
                "WHERE table_schema = ? ");
        statement.setString(1,schema);
        ResultSet resultSet = statement.executeQuery();

        while(resultSet.next()){
            tables.add(resultSet.getString("table_name"));
        }
        return  tables;
    }

    public Vector<String> listColumns(String tableName) throws SQLException {
        Vector<String> columns = new Vector<String>();
        PreparedStatement statement = this.connection.prepareStatement("" +
                "SELECT column_name " +
                "FROM INFORMATION_SCHEMA.columns " +
                "WHERE table_name = ? ");
        statement.setString(1,tableName);
        ResultSet resultSet = statement.executeQuery();

        while(resultSet.next()){
            columns.add(resultSet.getString("column_name"));
        }
        return  columns;
    }

    public String beginIsolationWithSnapshot() throws SQLException{
        Statement statement = this.getConnection().createStatement();

        // Setting Snapshot for Isolated copy for DB
        statement.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;");
        ResultSet rs = statement.executeQuery("SELECT pg_catalog.pg_export_snapshot();");
        rs.next();
        return rs.getString("pg_export_snapshot");
    }

    public void preSourceTasks(String tableName) throws SQLException {
        if (numberOfThreads != 1) {
            /**
             * Calculating the chunk size for parallel job processing
             */
            Statement statement = this.getConnection().createStatement();
            try {

                String sql = "SELECT " +
                        " abs(count(*) / " + numberOfThreads + ") chunk_size" +
                        ", count(*) total_rows" +
                        " FROM ";

                sql = sql + tableName;

                LOG.debug("Calculating the chunks size with this sql: " + sql);
                ResultSet rs = statement.executeQuery(sql);
                rs.next();
                chunkSize = rs.getLong(1);
                long totalNumberRows = rs.getLong(2);
                LOG.debug("chunkSize: " + chunkSize + " totalNumberRows: " + totalNumberRows);

                statement.close();
            } catch (Exception throwable) {
                statement.close();
                this.connection.rollback();
                throw throwable;
            }
        }
    }


    //   WRITE TABLE
//    @Override
    public int insertDataToTable(ResultSet resultSet,String tableName, int taskId) throws SQLException, IOException {
        CopyIn copyIn = null;

        try {

            ResultSetMetaData rsmd = resultSet.getMetaData();

            String allColumns = getColumnsFromResultSetMetaData(rsmd);

            // Get Postgres COPY meta-command manager
            PgConnection copyOperationConnection = this.connection.unwrap(PgConnection.class);
            CopyManager copyManager = new CopyManager(copyOperationConnection);
            String copyCmd = getCopyCommand(tableName, allColumns);
            copyIn = copyManager.copyIn(copyCmd);

            char unitSeparator = 0x1F;
            int columnsNumber = rsmd.getColumnCount();

            StringBuilder row = new StringBuilder();
            StringBuilder cols = new StringBuilder();

            byte[] bytes;
            String colValue;

            if (resultSet.next()) {
                do {

                    // Get Columns values
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i > 1) cols.append(unitSeparator);

                        switch (rsmd.getColumnType(i)) {
//                          TODO: Resolve Other Types
                            case Types.BLOB:
                                colValue = QueryHelper.blobToPostgresHex(resultSet.getBlob(i));
                                break;
                            default:
                                colValue = resultSet.getString(i);
                                break;
                        }

                        if (!resultSet.wasNull() || colValue != null) cols.append(colValue);
                    }

                    row.append(cols.toString());

                    // Row ends with \n
                    row.append("\n");

                    // Copy data to postgres
                    bytes = row.toString().getBytes(StandardCharsets.UTF_8);
                    copyIn.writeToCopy(bytes, 0, bytes.length);

                    // Clear StringBuilders
                    row.setLength(0); // set length of buffer to 0
                    row.trimToSize();
                    cols.setLength(0); // set length of buffer to 0
                    cols.trimToSize();
                } while (resultSet.next());
            }

            copyIn.endCopy();

        } catch (Exception e) {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
            this.connection.rollback();
            throw e;
        } finally {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
        }
        this.getConnection().commit();

        return 0;
    }

//    @Override
    public int insertDataToTableUsingCopy(ByteArrayOutputStream bout, String tableName, int taskId) throws SQLException, IOException {
        CopyIn copyIn = null;
        try {
            // Get Postgres COPY meta-command manager
            PgConnection copyOperationConnection = this.connection.unwrap(PgConnection.class);
            CopyManager copyManager = new CopyManager(copyOperationConnection);
            String copyCmd = getCopyCommand(tableName, null);
            copyIn = copyManager.copyIn(copyCmd);

            byte[] bytes=bout.toByteArray();
            copyIn.writeToCopy(bytes,0,bytes.length);

            copyIn.endCopy();

        } catch (Exception e) {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
            this.connection.rollback();
            throw e;
        } finally {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
        }
        this.getConnection().commit();
        return 0;
    }


    private String getColumnsFromResultSetMetaData(ResultSetMetaData rsmd) throws SQLException, IOException {

        StringBuilder columnNames = new StringBuilder();

        int columnsNumber = rsmd.getColumnCount();

        for (int i = 1; i <= columnsNumber; i++) {
            if (i > 1) columnNames.append(",");

            columnNames.append(rsmd.getColumnName(i));

        }
        return columnNames.toString();
    }

    private String getCopyCommand(String tableName, String allColumns) {

        StringBuilder copyCmd = new StringBuilder();

        copyCmd.append("COPY ");
        copyCmd.append(tableName);

        if (allColumns != null) {
            copyCmd.append(" (");
            copyCmd.append(allColumns);
            copyCmd.append(")");
        }

        copyCmd.append(" FROM STDIN WITH DELIMITER e'\\x1f'  NULL '' ENCODING 'UTF-8' ");

        LOG.info("Copying data with this command: " + copyCmd.toString());

        return copyCmd.toString();
    }



    public void isolateAndSetSnapshot(String snapshotID) throws SQLException {
        Statement statement = this.getConnection().createStatement();
        statement.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;");
        statement.execute("SET TRANSACTION SNAPSHOT '"+snapshotID+"';");
    }


//    READ TABLE
    public PreparedStatement getReadTableStatement(String tableName, String[] columns, int nThread) throws SQLException {

    String allColumns = columns == null ? "*" : escapeColumnNames(columns);

    String sqlCmd = "SELECT " +
            allColumns +
            " FROM " +
            escapeTableName(tableName);

    sqlCmd = sqlCmd + " OFFSET ? ";

    String limit = " LIMIT ?";

    PreparedStatement statement;

    long offset = nThread * chunkSize;
    if (numberOfThreads == nThread + 1) {
        statement = getExecuteStatement(sqlCmd, offset);
    } else {
        sqlCmd = sqlCmd + limit;
        statement = getExecuteStatement(sqlCmd, offset, chunkSize);
    }
    return statement;
}

    public ResultSet readTable(String tableName, String[] columns, int nThread) throws SQLException{
        PreparedStatement statement = getReadTableStatement(tableName, columns, nThread);
        return statement.executeQuery();
    }

    public ByteArrayOutputStream readTableUsingCopy(String tableName, String[] columns, int nThread) throws SQLException {
        PreparedStatement statement = getReadTableStatement(tableName, columns, nThread);
        PgConnection copyOperationConnection = this.connection.unwrap(PgConnection.class);
        CopyManager copyManager = new CopyManager(copyOperationConnection);
        ByteArrayOutputStream bout=new ByteArrayOutputStream();
        try {
            copyManager.copyOut(getCopyOutCmd(statement.toString()), (OutputStream)bout);
        } catch (IOException e) {
            LOG.error(e);
        }
        return bout;
    }

    /**
     * Executes an arbitrary SQL statement.
     *
     * @param stmt      The SQL statement to execute
     * @param fetchSize Overrides default or parameterized fetch size
     * @return A ResultSet encapsulating the results or null on error
     */
    protected PreparedStatement getExecuteStatement(String stmt, Integer fetchSize, Object... args)
            throws SQLException {
        // Release any previously-open statement.
        release();

        PreparedStatement statement = this.getConnection().prepareStatement(stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        if (fetchSize != null) {
            LOG.debug(Thread.currentThread().getName() + ": Using fetchSize for next query: " + fetchSize);
            statement.setFetchSize(fetchSize);
        }
        this.lastStatement = statement;

//        If any extra params need to be inserted in query
        if (null != args) {
            for (int i = 0; i < args.length; i++) {
                statement.setObject(i + 1, args[i]);
            }
        }

        LOG.info(Thread.currentThread().getName() + ": Executing SQL statement: " + stmt);
        StringBuilder sb = new StringBuilder();
        for (Object o : args) {
            sb.append(o.toString())
                    .append(", ");
        }
        LOG.info(Thread.currentThread().getName() + ": With args: " + sb.toString());

        return statement;
    }

    /**
     * Executes an arbitrary SQL Statement.
     *
     * @param stmt The SQL statement to execute
     * @return A ResultSet encapsulating the results or null on error
     */
    protected PreparedStatement getExecuteStatement(String stmt, Object... args) throws SQLException {
        return getExecuteStatement(stmt, fetchSize, args);
    }



    private String getCopyOutCmd(String sql){
        StringBuilder copyCmd = new StringBuilder();

        copyCmd.append("COPY (");
        copyCmd.append(sql);
        copyCmd.append(") TO STDOUT WITH DELIMITER e'\\x1f'  NULL '' ENCODING 'UTF-8' ");

        LOG.info("Copying data with this command: " + copyCmd.toString());

        return copyCmd.toString();
    }

    public void release() {
        if (null != this.lastStatement) {
            try {
                this.lastStatement.close();
            } catch (SQLException e) {
                LOG.error("Exception closing executed Statement: " + e, e);
            }

            this.lastStatement = null;
        }
    }

    /**
     * When using a table name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a table named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param tableName the table name as provided by the user, etc.
     * @return how the table name should be rendered in the sql text.
     */
    public String escapeTableName(String tableName) {
        return tableName;
    }
    private String escapeColumnNames(String[] columns) {
        return columns.toString().substring(1, columns.length-1);
    }


    public void endIsolation() throws SQLException{
        Statement statement = this.getConnection().createStatement();
        statement.execute("END;");
        statement.close();
    }

    public void closeConnection() throws SQLException{
            release();
            // Close connection, ignore exceptions
            if (this.connection != null) {
                try {
                    this.getConnection().close();
                } catch (Exception e) {
                    LOG.error(e);
                }
            }
    }
}
