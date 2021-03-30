import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CompletionStage;


public class BOSStream {

    public static void main(String[] args) throws IOException, SQLException {
        Properties props = new Properties();
        props.setProperty("user","postgres");
        props.setProperty("password","postgres");

        String sourceUrl = "jdbc:postgresql";
        String sinkUrl = "jdbc:postgresql";
        String tableName = "emp";

        ActorSystem actorSystem = ActorSystem.create();
        PostgresDB sourceDB = new PostgresDB(sourceUrl,props);
        PostgresDB sinkDB = new PostgresDB(sinkUrl,props);

        Source<Tuple2<ByteArrayOutputStream, Integer>, NotUsed> source = Source.range(0,25).map(taskId -> {
            ByteArrayOutputStream bout = sourceDB.readTableUsingCopy("users", null, taskId);
            System.out.println("Read:"+taskId);
            Tuple2<ByteArrayOutputStream, Integer> t2 = new Tuple2(bout,taskId);
            return t2;
        });

        Sink<Tuple2<ByteArrayOutputStream, Integer>, CompletionStage<Done>> sink = Sink.foreach(t2 -> {
            sinkDB.insertDataToTableUsingCopy(t2._1, "target", t2._2);
            System.out.println("Written:"+t2._2);
        });

        CompletionStage<Done> done = source.toMat(sink.async(), Keep.right()).run(actorSystem);

        done.whenComplete((done1,throwable)->{
           actorSystem.terminate();
        });

    }

}
