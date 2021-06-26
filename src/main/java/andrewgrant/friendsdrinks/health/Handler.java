package andrewgrant.friendsdrinks.health;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;


/**
 * Handler for simple Kafka streams health check.
 */
public class Handler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(Handler.class);

    private KafkaStreams kafkaStreams;

    public Handler(KafkaStreams kafkaStreams) {
        log.debug("In Handler constructor");
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        log.debug("In handle method");
        KafkaStreams.State state = kafkaStreams.state();
        logMetrics();
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
        outputStream.close();
        log.debug("Flushed output stream");
    }

    private void logMetrics() {
        for (Map.Entry<MetricName, ? extends Metric> entry : kafkaStreams.metrics().entrySet()) {
            log.debug("MetricName {} MetricValue {}", entry.getKey(), entry.getValue().toString());
        }
    }
}
