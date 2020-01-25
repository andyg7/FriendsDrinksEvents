package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Service responsible for writing to the email topic.
 * See Single Writer Principle https://www.confluent.io/blog/build-services-backbone-events/
 */
public class WriterService {
    private static final Logger log = LoggerFactory.getLogger(WriterService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("email_writer_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        return props;
    }

    public Topology buildTopology(Properties envProps,
                                  UserAvro userAvro,
                                  EmailAvro emailAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopicName = envProps.getProperty("user.topic.name");
        KStream<UserId, UserEvent> userEventKStream = builder.stream(userTopicName,
                userAvro.consumedWith());

        KStream<String, CreateUserRequest> createUserRequests =
                userEventKStream.filter(((key, value) ->
                        value.getEventType().equals(EventType.CREATE_USER_REQUEST)))
                        .mapValues(value -> value.getCreateUserRequest())
                        .selectKey((key, value) -> value.getRequestId());

        KStream<String, CreateUserResponse> createUserResponses =
                userEventKStream.filter(((key, value) ->
                        value.getEventType().equals(EventType.CREATE_USER_RESPONSE)))
                        .mapValues(value -> value.getCreateUserResponse())
                        .selectKey((key, value) -> value.getRequestId());

        KStream<UserId, EmailEvent> createUserEmailEvent = createUserResponses
                .join(createUserRequests,
                        (leftValue, rightValue) -> {
                            EmailEvent email = new EmailEvent();
                            email.setEmailId(new EmailId(rightValue.getEmail()));
                            email.setUserId(rightValue.getUserId().getId());
                            if (leftValue.getResult().equals(Result.SUCCESS)) {
                                email.setEventType(andrewgrant.friendsdrinks.email.avro
                                        .EventType.RESERVED);
                                return email;
                            } else if (leftValue.getResult().equals(Result.FAIL)){
                                email.setEventType(andrewgrant.friendsdrinks.email.avro
                                        .EventType.REJECTED);
                                return email;
                            } else {
                                throw new RuntimeException(
                                        String.format("Received unknown Result %s",
                                                leftValue.getResult().toString()));
                            }
                        },
                        JoinWindows.of(Duration.ofSeconds(10)),
                        Joined.with(
                                Serdes.String(),
                                userAvro.createUserResponseSerde(),
                                userAvro.createUserRequestSerde()))
                .selectKey((key, value) -> new UserId(value.getUserId()));


        final String emailTopic = envProps.getProperty("email.topic.name");
        // Re-key on email before publishing to email topic.
        KStream<EmailId, EmailEvent> createUserEmailEventKeyedByEmailId =
                createUserEmailEvent.selectKey(((key, value) -> value.getEmailId()));

        createUserEmailEventKeyedByEmailId.to(emailTopic, emailAvro.producedWith());

        KStream<UserId, DeleteUserResponse> deleteUserResponses =
                userEventKStream.filter(((key, value) ->
                value.getEventType().equals(EventType.DELETE_USER_RESPONSE) &&
                        value.getDeleteUserResponse().getResult().equals(Result.SUCCESS)
        )).mapValues(value -> value.getDeleteUserResponse());

        KStream<UserId, EmailEvent> emailsKeyedByUserId = builder.stream(emailTopic,
                emailAvro.consumedWith())
                .filter(((key, value) -> value.getEventType()
                        .equals(andrewgrant.friendsdrinks.email.avro.EventType.RESERVED)))
                .selectKey((key, value) -> new UserId(value.getUserId()));

        final String emailPrivateTopic = envProps.getProperty("emailPrivate3.topic.name");
        emailsKeyedByUserId.to(emailPrivateTopic,
                Produced.with(
                        userAvro.userIdSerde(),
                        emailAvro.emailEventSerde()));

        KTable<UserId, EmailEvent> emailKTableKeyedByUserId = builder.table(emailPrivateTopic,
                Consumed.with(userAvro.userIdSerde(),
                        emailAvro.emailEventSerde()));

        KStream<UserId, DeleteResponseAndCurrEmail> deleteResponseAndCurrEmailKStream =
                deleteUserResponses.join(emailKTableKeyedByUserId,
                        DeleteResponseAndCurrEmail::new,
                        Joined.with(userAvro.userIdSerde(),
                                userAvro.deleteUserResponseSerde(),
                                emailAvro.emailEventSerde()));

        KStream<EmailId, EmailEvent> returnedEmailKStream = deleteResponseAndCurrEmailKStream
                .mapValues(value ->
                        EmailEvent.newBuilder(value.getCurrEmailState())
                                .setEventType(andrewgrant.friendsdrinks.email.avro
                                        .EventType.RETURNED)
                                .build())
                .selectKey((key, value) -> value.getEmailId());

        returnedEmailKStream.to(emailTopic, emailAvro.producedWith());

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        WriterService writerService = new WriterService();
        Properties envProps = loadEnvProperties(args[0]);
        UserAvro userAvro =
                new UserAvro(envProps.getProperty("schema.registry.url"));
        EmailAvro emailAvro =
                new EmailAvro(envProps.getProperty("schema.registry.url"));
        Topology topology = writerService.buildTopology(envProps,
                userAvro, emailAvro);
        log.debug("Built stream");
        log.info("Topology description:\n {}", topology.describe());

        Properties streamProps = writerService.buildStreamsProperties(envProps);
        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
