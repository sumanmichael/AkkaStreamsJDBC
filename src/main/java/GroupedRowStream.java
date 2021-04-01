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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;


public class GroupedRowStream {

    public static void main(String[] args) throws IOException, SQLException {


        ActorSystem actorSystem = ActorSystem.create();

        class DBRow{
            int a;
            int b;
            int c;
            List<Integer> row;

            public DBRow(int a, int b, int c) {
                this.a = a;
                this.b = b;
                this.c = c;
                this.row = List.of(a,b,c);
            }

            @Override
            public String toString() {
                return "DBRow{" +
                        "a=" + a +
                        ", b=" + b +
                        ", c=" + c +
                        '}';
            }
        }

//        Source< DBRow, NotUsed> source = Source.range(1,297,3).map((i) -> new DBRow(i,i+1,i+2));

        final SlickSession sourceSession = SlickSession.forConfig("slick-postgres-source");
        final SlickSession sinkSession = SlickSession.forConfig("slick-postgres-sink");

        Source<DBRow, NotUsed> slickSource = Slick.source(
                sourceSession,
                "SELECT * from source;",
                (SlickRow row) -> {
                    return new DBRow(row.nextInt(), row.nextInt(), 0);
                });

//        Flow<DBRow,List<DBRow>, NotUsed> groupingFlow = Flow.of(DBRow.class).grouped(3);

        Sink<List<DBRow>,CompletionStage<Done>> sink = Slick.sink(
                sinkSession,
                (dbrows,connection)->{
                    CopyIn copyIn = null;
                    try {
                        PgConnection copyOperationConnection = connection.unwrap(PgConnection.class);
                        CopyManager copyManager = new CopyManager(copyOperationConnection);
                        String allColumns = null;
                        String targetTableName = "target";
                        String copyCmd = getCopyCommand(targetTableName, allColumns);
                        copyIn = copyManager.copyIn(copyCmd);

                        char unitSeparator = 0x1F;
                        int columnsNumber = 3;

                        StringBuilder row = new StringBuilder();
                        StringBuilder cols = new StringBuilder();

                        byte[] bytes;
                        String colValue;

                        for(DBRow r : dbrows){
                            // Get Columns values
                            for (int i = 1; i <= columnsNumber; i++) {
                                if (i > 1) cols.append(unitSeparator);

                                switch (Types.INTEGER) {
//                          TODO: Resolve Other Types
                                    default:
                                        colValue = r.row.get(i-1).toString();
                                        break;
                                }

                                if (colValue != null) cols.append(colValue);
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
                        }

                        copyIn.endCopy();
                    } catch (Exception e) {
                        System.out.println(e);
                        if (copyIn != null && copyIn.isActive()) {
                            copyIn.cancelCopy();
                        }
//                        connection.rollback();
                        e.printStackTrace();
                    } finally {
                        if (copyIn != null && copyIn.isActive()) {
                            copyIn.cancelCopy();
                        }
                    }
                    PreparedStatement statement = connection.prepareStatement("update target set a = 1 where 1=0");
                    return statement;
                }
        );

        long startTime = System.currentTimeMillis();

        CompletionStage<Done> done =
                slickSource
                        .grouped(20000).async()
                        .toMat(sink, Keep.right())
//                                    .toMat(Sink.foreach(System.out::println), Keep.right())
                        .run(actorSystem);

        done.whenComplete((done1,throwable)->{
            System.out.println("Total Time:"+(System.currentTimeMillis() - startTime));
            actorSystem.terminate();
        });

    }

    private static String getCopyCommand(String tableName, String allColumns) {

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


}
