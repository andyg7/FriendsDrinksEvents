package andrewgrant.friendsdrinks.frontend.api;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.*;
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
    private KafkaProducer<UserId, UserEvent> userProducer;
    private String userTopicName;

    public Service(KafkaProducer<UserId, UserEvent> userProducer,
                   String userTopicName) {
        this.userProducer = userProducer;
        this.userTopicName = userTopicName;
    }

    public Topology buildTopology(Properties envProps,
                                  UserAvro userAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopicName = envProps.getProperty("user.topic.name");
        builder.table(userTopicName,
                userAvro.consumedWith());

        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("frontend_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        return props;
    }

    @POST
    @Path("/user")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public String createUser(CreateUserRequestBean createUserRequest)
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
        ProducerRecord<UserId, UserEvent> record =
                new ProducerRecord<>(
                        userTopicName,
                        userEvent.getCreateUserRequest().getUserId(),
                        userEvent);
        userProducer.send(record).get();
        return requestId;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException {
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
        Service service = new Service(userProducer, envProps.getProperty("user.topic.name"));
        Topology topology = service.buildTopology(envProps, userAvro);
        Properties streamProps = service.buildStreamsProperties(envProps);
        KafkaStreams streams = new KafkaStreams(topology, streamProps);
        int port = Integer.parseInt(args[1]);
        service.startRestService(port);
        service.startStreams(streams);
    }

    private void startRestService(int port) {
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
            jettyServer.join();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Listening on " + jettyServer.getURI());
    }

    private void startStreams(KafkaStreams streams)
            throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        streams.start();
        latch.await();
    }
}
