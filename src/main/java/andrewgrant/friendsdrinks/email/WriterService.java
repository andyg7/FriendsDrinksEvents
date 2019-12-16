package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        final String userTopic = envProps.getProperty("user.topic.name");
        KStream<UserId, UserEvent> userEventKStream = builder.stream(userTopic,
                Consumed.with(userAvro.userIdSerde(),
                        userAvro.userEventSerde()));

        KStream<UserId, EmailEvent> createUserEmailEvent = userEventKStream.filter(((key, value) ->
                value.getEventType().equals(EventType.CREATE_USER_RESPONSE)
        )).mapValues((key, value) -> {
            EmailEvent email = new EmailEvent();
            CreateUserResponse response = value.getCreateUserResponse();
            email.setEmailId(new EmailId(response.getEmail()));
            email.setUserId(response.getUserId().getId());
            if (value.getCreateUserResponse().getResult().equals(Result.SUCCESS)) {
                email.setEventType(andrewgrant.friendsdrinks.email.avro
                        .EventType.RESERVED);
                return email;
            } else if (value.getCreateUserResponse().getResult().equals(Result.FAIL)) {
                email.setEventType(andrewgrant.friendsdrinks.email.avro
                        .EventType.REJECTED);
                return email;
            } else {
                throw new RuntimeException(String.format("Received unknown Result %s",
                        value.getCreateUserResponse().getResult().toString()));
            }
        });

        KStream<UserId, DeleteUserResponse> deleteUserResponses =
                userEventKStream.filter(((key, value) ->
                value.getEventType().equals(EventType.DELETE_USER_RESPONSE) &&
                        value.getDeleteUserResponse().getResult().equals(Result.SUCCESS)
        )).mapValues(value -> value.getDeleteUserResponse());

        final String emailTopic = envProps.getProperty("email.topic.name");
        KStream<UserId, EmailEvent> emailsKeyedByUserId = builder.stream(emailTopic,
                Consumed.with(emailAvro.emailIdSerde(),
                        emailAvro.emailSerde()))
                .filter(((key, value) -> value.getEventType()
                        .equals(andrewgrant.friendsdrinks.email.avro.EventType.RESERVED)))
                .selectKey((key, value) -> new UserId(value.getUserId()));

        final String emailTmpTopic = envProps.getProperty("email_tmp_3.topic.name");
        emailsKeyedByUserId.to(emailTmpTopic,
                Produced.with(
                        userAvro.userIdSerde(),
                        emailAvro.emailSerde()));

        KTable<UserId, EmailEvent> emailKTableKeyedByUserId = builder.table(
                emailTmpTopic,
                Consumed.with(userAvro.userIdSerde(),
                        emailAvro.emailSerde()));

        KStream<UserId, DeleteResponseAndCurrEmail> deleteResponseAndCurrEmailKStream =
                deleteUserResponses.join(emailKTableKeyedByUserId,
                        DeleteResponseAndCurrEmail::new,
                        Joined.with(userAvro.userIdSerde(),
                                userAvro.deleteUserResponseSerde(),
                                emailAvro.emailSerde()));

        KStream<EmailId, EmailEvent> returnedEmailKStream = deleteResponseAndCurrEmailKStream
                .mapValues(value ->
                        EmailEvent.newBuilder(value.getCurrEmailState())
                                .setEventType(andrewgrant.friendsdrinks.email.avro
                                        .EventType.RETURNED)
                                .build())
                .selectKey((key, value) -> value.getEmailId());

        returnedEmailKStream.to(emailTopic,
                Produced.with(emailAvro.emailIdSerde(),
                        emailAvro.emailSerde()));

        // Re-key on email before publishing to email topic.
        KStream<EmailId, EmailEvent> createUserEmailEventKeyedByEmailId =
                createUserEmailEvent.selectKey(((key, value) -> value.getEmailId()));

        createUserEmailEventKeyedByEmailId.to(emailTopic,
                Produced.with(emailAvro.emailIdSerde(),
                        emailAvro.emailSerde()));
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        WriterService writerService = new WriterService();
        Properties envProps = loadEnvProperties(args[0]);
        UserAvro userAvro = new UserAvro(envProps, null);
        EmailAvro emailAvro = new EmailAvro(envProps, null);
        Topology topology = writerService.buildTopology(envProps,
                userAvro, emailAvro);
        log.debug("Built stream");

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
