package andrewgrant.friendsdrinks.health;

import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Server for health check.
 */
public class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    public static org.eclipse.jetty.server.Server buildServer(int port, KafkaStreams kafkaStreams) {
        // Jetty server context handler.
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/v1");
        final org.eclipse.jetty.server.Server jettyServer = new org.eclipse.jetty.server.Server();
        ServerConnector serverConnector = new ServerConnector(jettyServer);
        serverConnector.setHost("0.0.0.0");
        serverConnector.setPort(port);
        jettyServer.addConnector(serverConnector);
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(new Handler(kafkaStreams));
        rc.register(JacksonFeature.class);
        // Jersey container for ResourceConfig.
        final ServletContainer sc = new ServletContainer(rc);
        // Jetty class that holds sc which is an
        // implementation of the javax Servlet interface.
        context.addServlet(new ServletHolder(sc), "/*");

        return jettyServer;
    }

    public static void start(org.eclipse.jetty.server.Server server, int port) {
        try {
            server.start();
            URI uri = server.getURI();
            if (uri.getPort() != port) {
                throw new RuntimeException(String.format("Failed to bind to port %d. " +
                        "Instead we're listening on %d", port, uri.getPort()));
            }
            log.info("Listening on " + server.getURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void stop(org.eclipse.jetty.server.Server server) {
        try {
            server.stop();
        } catch (Exception e) {
            log.error("Error from starting health check server: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
