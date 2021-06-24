package andrewgrant.friendsdrinks.health;

import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;


/**
 * Handler for simple Kafka streams health check.
 */
public class Handler implements HttpHandler {

    private KafkaStreams kafkaStreams;

    public Handler(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        KafkaStreams.State state = kafkaStreams.state();
        if (!state.isRunningOrRebalancing()) {
            throw new RuntimeException(String.format("State is %s", state.name()));
        }
        String resp = "Success!";
        httpExchange.sendResponseHeaders(200, resp.length());
        OutputStream outputStream = httpExchange.getResponseBody();
        outputStream.write(resp.getBytes());
        outputStream.flush();
        outputStream.close();;
    }
}
