package andrewgrant.friendsdrinks.frontend.api;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import andrewgrant.friendsdrinks.email.EmailAvro;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;

/**
 * Hooks up dependencies for the Frontend REST API.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Program expects " +
                    "1) path to config 2) app port");
        }

        Properties envProps = loadEnvProperties(args[0]);
        UserAvro userAvro = new UserAvro(
                envProps.getProperty("schema.registry.url"));
        EmailAvro emailAvro =
                new EmailAvro(envProps.getProperty("schema.registry.url"));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        KafkaProducer<UserId, UserEvent> userProducer =
                new KafkaProducer<>(
                        producerProps,
                        userAvro.userIdSerializer(),
                        userAvro.userEventSerializer());
        String portStr = args[1];
        String streamsUri = "localhost:" + portStr;
        StreamsService streamsService = new StreamsService(envProps,
                streamsUri,
                userAvro,
                emailAvro);
        KafkaStreams streams = streamsService.getStreams();

        Main.startStreams(streams);
        Thread.sleep(10000);

        int port = Integer.parseInt(portStr);
        Handler handler = new Handler(userProducer, envProps, streamsService);
        Server jettyServer = Main.createServer(handler, port);
        URI uri = jettyServer.getURI();
        if (uri.getPort() != port) {
            throw new RuntimeException(String.format("Failed to bind to port %d. " +
                    "Instead we're listening on %d", port, uri.getPort()));
        }
        log.info("Started server and streams");
        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streamsService.close();
                try {
                    jettyServer.stop();
                } catch (Exception e){
                    throw new RuntimeException(e);
                }
                userProducer.close();
            }
        });
        streams.start();
        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Listening on " + jettyServer.getURI());
        Thread.currentThread().join();
    }

    private static Server createServer(Handler handler,
                                       int port) {
        final ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");

        final ResourceConfig rc = new ResourceConfig();
        rc.register(handler);
        rc.register(JacksonFeature.class);
        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);

        context.addServlet(holder, "/*");

        final Server jettyServer = new Server(port);
        jettyServer.setHandler(context);
        return jettyServer;
    }

    private static void startStreams(KafkaStreams streams) {
        // Attach shutdown handler to catch Control-C.
        streams.start();
    }
}
