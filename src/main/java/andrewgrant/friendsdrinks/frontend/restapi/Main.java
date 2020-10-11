package andrewgrant.friendsdrinks.frontend.restapi;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
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

import andrewgrant.friendsdrinks.AvroBuilder;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.user.UserAvroBuilder;
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

        log.info("Starting Frontend API application");

        Properties envProps = load(args[0]);
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        AvroBuilder avroBuilder = new AvroBuilder(schemaRegistryUrl);
        andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder =
                new andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder(schemaRegistryUrl);
        UserAvroBuilder userAvroBuilder = new UserAvroBuilder(schemaRegistryUrl);

        String portStr = args[1];
        String streamsUri = "localhost:" + portStr;
        StreamsService streamsService = new StreamsService(envProps, streamsUri, avroBuilder, apiAvroBuilder,
                new andrewgrant.friendsdrinks.membership.AvroBuilder(schemaRegistryUrl), new UserAvroBuilder(schemaRegistryUrl));
        KafkaStreams streams = streamsService.getStreams();
        int port = Integer.parseInt(portStr);
        Server jettyServer = Main.buildServer(envProps, streams, userAvroBuilder, apiAvroBuilder, port);
        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                try {
                    jettyServer.stop();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        streams.start();
        try {
            jettyServer.start();
            URI uri = jettyServer.getURI();
            if (uri.getPort() != port) {
                throw new RuntimeException(String.format("Failed to bind to port %d. " +
                        "Instead we're listening on %d", port, uri.getPort()));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Listening on " + jettyServer.getURI());
        Thread.currentThread().join();
    }

    private static KafkaProducer<String, FriendsDrinksEvent> buildFriendsDrinksProducer(
            Properties envProps,
            andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder avro) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return new KafkaProducer<>(
                producerProps,
                Serdes.String().serializer(),
                avro.friendsDrinksSerializer());
    }

    private static KafkaProducer<UserId, UserEvent> buildUserDrinksProducer(
            Properties envProps,
            UserAvroBuilder avro) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return new KafkaProducer<>(
                producerProps,
                avro.userIdSerializer(),
                avro.userEventSerializer());
    }

    private static Server buildServer(Properties envProps, KafkaStreams streams,
                                      UserAvroBuilder userAvroBuilder,
                                      andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                      int port) {
        // Jetty server context handler.
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/v1");
        final Server jettyServer = new Server(port);
        jettyServer.setHandler(context);

        context.addServlet(buildFriendsDrinksHolder(streams, userAvroBuilder, apiAvroBuilder, envProps), "/*");

        return jettyServer;
    }

    private static ServletHolder buildFriendsDrinksHolder(KafkaStreams streams,
                                                          UserAvroBuilder userAvroBuilder,
                                                          andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                                          Properties envProps) {
        KafkaProducer<String, FriendsDrinksEvent> friendsDrinksProducer =
                buildFriendsDrinksProducer(envProps, apiAvroBuilder);
        KafkaProducer<UserId, UserEvent> userProducer =
                buildUserDrinksProducer(envProps, userAvroBuilder);
        andrewgrant.friendsdrinks.frontend.restapi.api.Handler handler =
                new andrewgrant.friendsdrinks.frontend.restapi.api.Handler(
                        streams, friendsDrinksProducer, userProducer, envProps);
        final ResourceConfig rc = new ResourceConfig();
        rc.register(handler);
        rc.register(JacksonFeature.class);
        // Jersey container for ResourceConfig.
        final ServletContainer sc = new ServletContainer(rc);
        // Jetty class that holds sc which is an
        // implementation of the javax Servlet interface.
        return new ServletHolder(sc);
    }

}
