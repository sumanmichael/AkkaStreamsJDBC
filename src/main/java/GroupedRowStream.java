import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickRow;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletionStage;


public class GroupedRowStream {


    public void migrateThroughStream(String sourceTableName, String sinkTableName) throws SQLException {
        ActorSystem actorSystem = ActorSystem.create();

        SlickSession sourceSession = SlickSession.forConfig("slick-postgres-source");
        SlickSession sinkSession = SlickSession.forConfig("slick-postgres-sink");

        Map<String, JDBCType> columnsMetaData = getColumnsMetaData(sinkSession,sourceTableName);

        String allColumns = null;
        String copyCmd = getCopyCommand(sinkTableName, allColumns);

        Source<String, NotUsed> slickSource = Slick.source(
                sourceSession,
                "SELECT * from "+sourceTableName+";",
                (SlickRow row) -> getStringFromRow(columnsMetaData, row)
        );

        Flow<String,List<String>, NotUsed> groupingFlow = Flow.of(String.class).grouped(20000);

        Sink<List<String>,CompletionStage<Done>> sink = Slick.sink(sinkSession,(dbrows, connection) -> this.insertRowsUsingCopy(dbrows,connection, copyCmd));

        long startTime = System.currentTimeMillis();
        CompletionStage<Done> done = slickSource
                                            .via(groupingFlow)
                                            .toMat(sink, Keep.right())
                                            .run(actorSystem);

        done.whenComplete((done1,throwable)->{
            System.out.println("Total Time:"+(System.currentTimeMillis() - startTime));
            throwable.printStackTrace();
            actorSystem.terminate();
        });

    }

    private String getStringFromRow(Map<String, JDBCType> columnsMetaData, SlickRow row) {
        List<String> cols = new Vector<>(columnsMetaData.size());
        for(JDBCType type: columnsMetaData.values()){
            switch (type){
                case INTEGER:
                case NUMERIC:
                    cols.add(row.nextInt().toString());
                    break;
                case VARCHAR:
                    cols.add(row.nextString());
                    break;
            }
        }
        char unitSeparator = 0x1F;
        return String.join(Character.toString(unitSeparator),cols);
    }

    private Map<String, JDBCType> getColumnsMetaData(SlickSession slickSession, String tableName) throws SQLException {
        Map<String, JDBCType> columnsMetaData = new LinkedHashMap<>();
        Connection conn =slickSession.db().createSession().conn();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet columns = databaseMetaData.getColumns(null,null,tableName,null);
        while(columns.next()){
//            System.out.println(columns.getType());
//            System.out.println(columns.getInt("DATA_TYPE"));
            columnsMetaData.put(
                    columns.getString("COLUMN_NAME"),
                    JDBCType.valueOf(columns.getInt("DATA_TYPE"))
            );
        }
        conn.close();
        return columnsMetaData;
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

//        LOG.info("Copying data with this command: " + copyCmd.toString());

        return copyCmd.toString();
    }

    private PreparedStatement insertRowsUsingCopy(List<String> dbrows, Connection connection, String copyCmd) throws SQLException {
        CopyIn copyIn = null;
        try {
            PgConnection copyOperationConnection = connection.unwrap(PgConnection.class);
            CopyManager copyManager = new CopyManager(copyOperationConnection);
            copyIn = copyManager.copyIn(copyCmd);


            StringBuilder row = new StringBuilder();

            byte[] bytes;

            for (String colString : dbrows) {
                row.append(colString);
                row.append("\n");

                // Copy data to postgres
                bytes = row.toString().getBytes(StandardCharsets.UTF_8);
                copyIn.writeToCopy(bytes, 0, bytes.length);

                // Clear StringBuilders
                row.setLength(0);
                row.trimToSize();
            }

            copyIn.endCopy();
        } catch (Exception e) {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
            connection.rollback();
            e.printStackTrace();
            throw e;
        } finally {
            if (copyIn != null && copyIn.isActive()) {
                copyIn.cancelCopy();
            }
        }
//        TODO: auto commit off
//        connection.commit();

        PreparedStatement statement = connection.prepareStatement("update target set a = 1 where 1=0");
        return statement;
    }

    public static void main(String[] args) throws SQLException {
        new GroupedRowStream().migrateThroughStream("source","target");
    }

}
