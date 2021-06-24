package andrewgrant.friendsdrinks.health;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.sun.net.httpserver.HttpServer;

/**
 * Server for health check.
 */
public class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    public static void start(int port, KafkaStreams kafkaStreams) {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        server.createContext("/test", new Handler(kafkaStreams));
        server.setExecutor(threadPoolExecutor);
        server.start();
        log.info(" Server started on port 8001");
    }

    public static void stop(HttpServer server) {
        try {
            server.stop(60);
        } catch (Exception e) {
            log.error("Error from starting health check server: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }


}

