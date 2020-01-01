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
 * Service for validating create user requests based on email.
 */
public class DeleteUserValidationService {
    private static final Logger log = LoggerFactory.getLogger(DeleteUserValidationService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("email_delete_user_validation_application.id"));
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

        final String emailTopic = envProps.getProperty("email.topic.name");
        // Get stream of email events and clean up state store as they come in
        KStream<EmailId, EmailEvent> emailKStreamRaw = builder.stream(emailTopic,
                Consumed.with(
                        emailAvro.emailIdSerde(), emailAvro.emailEventSerde()));

        final String userTopicName = envProps.getProperty("user.topic.name");
        KStream<UserId, UserEvent> userIdKStream = builder.stream(userTopicName,
                userAvro.consumedWith());

        KStream<UserId, EmailEvent> emailStreamKeyedByUserId = emailKStreamRaw
                .selectKey(((key, value) -> new UserId(value.getUserId())));

        final String emailPrivate2Topic = envProps.getProperty("emailPrivate2.topic.name");
        emailStreamKeyedByUserId.to(emailPrivate2Topic,
                Produced.with(
                        userAvro.userIdSerde(),
                        emailAvro.emailEventSerde()));

        KTable<UserId, EmailEvent> emailTableKeyedByUserId = builder.table(emailPrivate2Topic,
                Consumed.with(userAvro.userIdSerde(),
                        emailAvro.emailEventSerde()));

        // Filter by requests so we have a stream of user requests.
        KStream<UserId, DeleteUserRequest> deleteUserRequests = userIdKStream.
                filter(((key, value) -> value.getEventType().equals(EventType.DELETE_USER_REQUEST)))
                .mapValues(value -> value.getDeleteUserRequest());

        KStream<UserId, DeleteRequestAndCurrEmail> deleteRequestAndEmail = deleteUserRequests
                .leftJoin(emailTableKeyedByUserId, DeleteRequestAndCurrEmail::new,
                        Joined.with(
                                userAvro.userIdSerde(),
                                userAvro.deleteUserRequestSerde(),
                                emailAvro.emailEventSerde()));

        KStream<UserId, UserEvent> validatedDeleteUser = deleteRequestAndEmail.mapValues(
                (key, value) -> {
                    if (value.getCurrEmailState() == null) {
                        DeleteUserRejected rejected = DeleteUserRejected.newBuilder()
                                .setUserId(value.getDeleteUserRequest().getUserId())
                                .setRequestId(value.getDeleteUserRequest().getRequestId())
                                .setErrorCode(ErrorCode.DoesNotExist.toString())
                                .build();
                        return UserEvent.newBuilder()
                                .setEventType(EventType.DELETE_USER_REJECTED)
                                .setDeleteUserRejected(rejected)
                                .build();
                    } else {
                        UserId userId = UserId.newBuilder()
                                .setId(value.getCurrEmailState().getUserId())
                                .build();
                        DeleteUserValidated validated = DeleteUserValidated.newBuilder()
                                .setRequestId(value.getDeleteUserRequest().getRequestId())
                                .setUserId(userId)
                                .build();
                        return UserEvent.newBuilder()
                                .setEventType(EventType.DELETE_USER_VALIDATED)
                                .setDeleteUserValidated(validated)
                                .build();
                    }
                });

        final String userValidationTopic = envProps.getProperty("userValidation.topic.name");
        validatedDeleteUser.to(userValidationTopic, userAvro.producedWith());

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        DeleteUserValidationService validationService = new DeleteUserValidationService();
        Properties envProps = loadEnvProperties(args[0]);
        UserAvro userAvro =
                new UserAvro(envProps.getProperty("schema.registry.url"));
        EmailAvro emailAvro =
                new EmailAvro(envProps.getProperty("schema.registry.url"));
        Topology topology = validationService.buildTopology(envProps,
                userAvro, emailAvro);

        log.debug("Built stream");
        log.info("Topology description:\n {}", topology.describe());

        Properties streamProps = validationService.buildStreamsProperties(envProps);
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
