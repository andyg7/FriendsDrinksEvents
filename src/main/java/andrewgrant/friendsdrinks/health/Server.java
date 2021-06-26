package andrewgrant.friendsdrinks.health;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;

/**
 * Server for health check.
 */
public class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    public static HttpServer buildServer(int port, KafkaStreams kafkaStreams) {
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        server.createContext("/v1/health", new Handler(kafkaStreams));
        server.setExecutor(null);
        return server;
    }

    public static void start(HttpServer server) {
        server.start();
        log.info("Server started on {}", server.getAddress().toString());
    }

    public static void stop(HttpServer server) {
        log.debug("Stopping server");
        try {
            server.stop(60);
        } catch (Exception e) {
            log.error("Error from starting health check server: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }


}

