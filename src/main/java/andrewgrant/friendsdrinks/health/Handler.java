package andrewgrant.friendsdrinks.health;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;


/**
 * Handler for simple Kafka streams health check.
 */
public class Handler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(Handler.class);

    private KafkaStreams kafkaStreams;

    public Handler(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        KafkaStreams.State state = kafkaStreams.state();
        if (!state.isRunningOrRebalancing()) {
            log.warn("Kafka streams is not running or re-balancing {}", state.name());
            throw new RuntimeException(String.format("State is %s", state.name()));
        }
        log.debug("Kafka streams is healthy with state {}", state.name());
        String resp = "Success! State is " + state.name();
        httpExchange.sendResponseHeaders(200, resp.length());
        OutputStream outputStream = httpExchange.getResponseBody();
        outputStream.write(resp.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
        outputStream.close();;
    }
}
