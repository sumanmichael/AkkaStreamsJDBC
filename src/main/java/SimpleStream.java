import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.OverflowStrategy;
import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickRow;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.sql.PreparedStatement;
import java.util.concurrent.CompletionStage;

class User{
    public int id;
    public String name;

    public User(int id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
public class SimpleStream {
    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create();
        long startTime = System.currentTimeMillis();
        final SlickSession sourceSession = SlickSession.forConfig("slick-postgres-source");
        final SlickSession sinkSession = SlickSession.forConfig("slick-postgres-sink");

        Source<User, NotUsed> source = Slick.source(
                sourceSession,
                "SELECT * from users;",
                (SlickRow row) -> new User(row.nextInt(), row.nextString()));

        Sink<User, CompletionStage<Done>> sink = Slick.sink(
                sinkSession,
                16,
                // add an optional second argument to specify the parallelism factor (int)
                (user, connection) -> {

                    PreparedStatement statement =
                            connection.prepareStatement(
                                    "INSERT INTO target VALUES (?, ?)");
                    statement.setInt(1, user.id);
                    statement.setString(2, user.name);
                    return statement;
                });
        CompletionStage<Done> done = source
                                        .toMat(sink, Keep.right())
                                        .run(actorSystem);


        done.whenComplete((done1, throwable) -> {
            System.out.println("Total Time:"+(System.currentTimeMillis() - startTime));
            actorSystem.terminate();
        });
    }

}
