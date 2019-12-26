package andrewgrant.friendsdrinks.frontend.api;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
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
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.CreateUserRequest;
import andrewgrant.friendsdrinks.user.avro.EventType;
import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Implements frontend REST API for interacting with backend.
 */
@Path("v1")
public class Service {

    private static final Logger log = LoggerFactory.getLogger(Service.class);
    private final String requestsStore = "requests-store";

    private KafkaProducer<UserId, UserEvent> userProducer;
    private Properties envProps;

    public Service(KafkaProducer<UserId, UserEvent> userProducer,
                   Properties envProps) {
        this.userProducer = userProducer;
        this.envProps = envProps;
    }

    public Topology buildTopology(UserAvro userAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        SessionWindows sessionWindows = SessionWindows.with(Duration.ofMinutes(10));
        final long oneMinute = 1000 * 60;
        sessionWindows = sessionWindows.until(oneMinute);
        final String userTopicName = envProps.getProperty("user.topic.name");
        final String frontendPrivateTopicName =
                envProps.getProperty("frontendPrivate.topic.name");
        builder.stream(userTopicName,
                userAvro.consumedWith())
                .filter(((key, value) ->
                        value.getEventType().equals(EventType.CREATE_USER_RESPONSE)))
                .mapValues(value -> value.getCreateUserResponse())
                .selectKey((key, value) -> value.getRequestId())
                .groupByKey(Grouped.with(
                        Serdes.String(),
                        userAvro.createUserResponseSerde()))
                .windowedBy(sessionWindows)
                .reduce((value1, value2) -> value1)
                .toStream((key, value) -> key.key())
                .to(frontendPrivateTopicName, Produced.with(Serdes.String(),
                        userAvro.createUserResponseSerde()));
        builder.table(frontendPrivateTopicName,
                Consumed.with(Serdes.String(), userAvro.createUserResponseSerde()),
                Materialized.as(requestsStore));

        return builder.build();
    }

    public Properties buildStreamsProperties(String uri) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("frontend_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, uri);
        return props;
    }

    @POST
    @Path("/user")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public CreateUserResponseBean createUser(final CreateUserRequestBean createUserRequest)
            throws ExecutionException, InterruptedException {
        String userIdStr = UUID.randomUUID().toString();
        UserId userId = UserId.newBuilder()
                .setId(userIdStr)
                .build();
        String requestId = UUID.randomUUID().toString();
        CreateUserRequest request = CreateUserRequest.newBuilder()
                .setEmail(createUserRequest.getEmail())
                .setUserId(userId)
                .setRequestId(requestId)
                .build();
        UserEvent userEvent = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(request)
                .build();
        final String userTopicName = envProps.getProperty("user.topic.name");
        ProducerRecord<UserId, UserEvent> record =
                new ProducerRecord<>(
                        userTopicName,
                        userEvent.getCreateUserRequest().getUserId(),
                        userEvent);
        userProducer.send(record).get();
        CreateUserResponseBean responseBean =
                new CreateUserResponseBean();
        responseBean.setRequestId(requestId);
        return responseBean;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Program expects " +
                    "1) path to config 2) app port");
        }

        Properties envProps = loadEnvProperties(args[0]);
        UserAvro userAvro = new UserAvro(
                envProps.getProperty("schema.registry.url"));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        KafkaProducer<UserId, UserEvent> userProducer =
                new KafkaProducer<>(
                        producerProps,
                        userAvro.userIdSerializer(),
                        userAvro.userEventSerializer());
        Service service = new Service(userProducer, envProps);
        Topology topology = service.buildTopology(userAvro);
        String portStr = args[1];
        String streamsUri = "localhost:" + portStr;
        Properties streamProps = service.buildStreamsProperties(streamsUri);
        KafkaStreams streams = new KafkaStreams(topology, streamProps);
        service.startStreams(streams);

        int port = Integer.parseInt(portStr);
        URI uri = service.startRestService(port);
        if (uri.getPort() != port) {
            throw new RuntimeException(String.format("Failed to bind to port %d. " +
                    "Instead we're listening on %d", port, uri.getPort()));
        }
    }

    private URI startRestService(int port) {
        final Server jettyServer = new Server(port);
        final ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);
        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);

        context.addServlet(holder, "/*");

        try {
            jettyServer.start();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Listening on " + jettyServer.getURI());
        return jettyServer.getURI();
    }

    private void startStreams(KafkaStreams streams) {
        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
            }
        });
        streams.start();
    }
}
