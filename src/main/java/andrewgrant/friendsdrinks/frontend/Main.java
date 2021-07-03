package andrewgrant.friendsdrinks.frontend;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
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

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService;

/**
 * Hooks up dependencies for the Frontend REST API.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Program requires path to config as first argument");
        }

        log.info("Starting Frontend API application");

        Properties envProps = load(args[0]);
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        andrewgrant.friendsdrinks.AvroBuilder avroBuilder = new andrewgrant.friendsdrinks.AvroBuilder(schemaRegistryUrl);
        andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder =
                new andrewgrant.friendsdrinks.frontend.AvroBuilder(schemaRegistryUrl);
        andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder =
                new andrewgrant.friendsdrinks.user.AvroBuilder(schemaRegistryUrl);
        andrewgrant.friendsdrinks.meetup.AvroBuilder meetupAvroBuilder =
                new andrewgrant.friendsdrinks.meetup.AvroBuilder(schemaRegistryUrl);

        int port = 8080;
        String portStr = String.valueOf(port);
        String streamsUri = "localhost:" + portStr;

        MaterializedViewsService streamsService = new MaterializedViewsService(
                envProps, avroBuilder, apiAvroBuilder,
                new andrewgrant.friendsdrinks.membership.AvroBuilder(schemaRegistryUrl),
                new andrewgrant.friendsdrinks.user.AvroBuilder(schemaRegistryUrl),
                new andrewgrant.friendsdrinks.meetup.AvroBuilder(schemaRegistryUrl));
        Topology topology = streamsService.buildTopology();
        Properties streamProps = streamsService.buildStreamsProperties(streamsUri);
        KafkaStreams streams = new KafkaStreams(topology, streamProps);
        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception {}", exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        Server jettyServer = Main.buildServer(envProps, streams, userAvroBuilder, apiAvroBuilder, meetupAvroBuilder, port);
        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                log.info("Running shutdown hook...");
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

    private static KafkaProducer<String, ApiEvent> buildFriendsDrinksProducer(
            Properties envProps,
            andrewgrant.friendsdrinks.frontend.AvroBuilder avro) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return new KafkaProducer<>(
                producerProps,
                Serdes.String().serializer(),
                avro.apiEventSerializer());
    }

    private static KafkaProducer<FriendsDrinksMeetupId, FriendsDrinksMeetupEvent> buildFriendsDrinksMeetupProducer(
            Properties envProps,
            andrewgrant.friendsdrinks.meetup.AvroBuilder avro) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return new KafkaProducer<>(
                producerProps,
                avro.friendsDrinksMeetupIdSerializer(),
                avro.friendsDrinksMeetupEventSerializer());
    }

    private static KafkaProducer<UserId, UserEvent> buildUserDrinksProducer(
            Properties envProps,
            andrewgrant.friendsdrinks.user.AvroBuilder avro) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return new KafkaProducer<>(producerProps, avro.userIdSerializer(), avro.userEventSerializer());
    }

    private static Server buildServer(Properties envProps, KafkaStreams streams,
                                      andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder,
                                      andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
                                      andrewgrant.friendsdrinks.meetup.AvroBuilder meetupAvroBuilder,
                                      int port) {
        // Jetty server context handler.
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/v1");
        final Server jettyServer = new Server();
        ServerConnector serverConnector = new ServerConnector(jettyServer);
        serverConnector.setHost("0.0.0.0");
        serverConnector.setPort(port);
        jettyServer.addConnector(serverConnector);
        jettyServer.setHandler(context);

        context.addServlet(buildFriendsDrinksHolder(streams, userAvroBuilder,
                apiAvroBuilder, meetupAvroBuilder, envProps), "/*");

        return jettyServer;
    }

    private static ServletHolder buildFriendsDrinksHolder(KafkaStreams streams,
                                                          andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder,
                                                          andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
                                                          andrewgrant.friendsdrinks.meetup.AvroBuilder meetupAvroBuilder,
                                                          Properties envProps) {
        KafkaProducer<String, ApiEvent> friendsDrinksProducer =
                buildFriendsDrinksProducer(envProps, apiAvroBuilder);
        KafkaProducer<UserId, UserEvent> userProducer =
                buildUserDrinksProducer(envProps, userAvroBuilder);
        KafkaProducer<FriendsDrinksMeetupId, FriendsDrinksMeetupEvent> friendsDrinksMeetupEventKafkaProducer =
                buildFriendsDrinksMeetupProducer(envProps, meetupAvroBuilder);
        andrewgrant.friendsdrinks.frontend.api.Handler handler =
                new andrewgrant.friendsdrinks.frontend.api.Handler(
                        streams, friendsDrinksProducer, userProducer, friendsDrinksMeetupEventKafkaProducer, envProps);
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
