package andrewgrant.friendsdrinks.user;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Validates delete user requests.
 */
public class ValidationService {
    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);

    public Topology buildTopology(Properties envProps) {

        StreamsBuilder builder = new StreamsBuilder();
        final String usersTopic = envProps.getProperty("user.topic.name");

        KStream<UserId, UserEvent> rawUserKStream = builder.stream(usersTopic,
                Consumed.with(AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUserEvent(envProps)));

        final String usersTmpTopic = envProps.getProperty("user_tmp.topic.name");
        rawUserKStream.filter(((key, value) -> value.getEventType()
                .equals(EventType.CREATE_USER_RESPONSE) &&
                value.getCreateUserResponse().getResult().equals(Result.SUCCESS)))
                .to(usersTmpTopic,
                        Produced.with(
                                AvroSerdeFactory.buildUserId(envProps),
                                AvroSerdeFactory.buildUserEvent(envProps)));

        KTable<UserId, UserEvent> userKTable = builder.table(usersTmpTopic,
                Consumed.with(AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUserEvent(envProps)));

        KStream<UserId, DeleteUserRequest> deleteRequestKStream = rawUserKStream
                .filter(((key, value) -> value.getEventType()
                        .equals(EventType.DELETE_USER_REQUEST)))
                .mapValues(value -> value.getDeleteUserRequest());

        KStream<UserId, UserEvent> validatedRequests = deleteRequestKStream.leftJoin(userKTable,
                (leftValue, rightValue) -> {
                    if (rightValue == null) {
                        DeleteUserRejected rejected = DeleteUserRejected.newBuilder()
                                .setErrorCode(ErrorCode.DOES_NOT_EXIST.toString())
                                .setUserId(null)
                                .setRequestId(leftValue.getRequestId())
                                .build();
                        return UserEvent.newBuilder()
                                .setEventType(EventType.DELETE_USER_REJECTED)
                                .setDeleteUserRejected(rejected)
                                .build();
                    } else {
                        DeleteUserValidated validated = DeleteUserValidated.newBuilder()
                                .setUserId(leftValue.getUserId())
                                .setRequestId(leftValue.getRequestId())
                                .build();
                        return UserEvent.newBuilder()
                                .setEventType(EventType.DELETE_USER_VALIDATED)
                                .setDeleteUserValidated(validated)
                                .build();
                    }
                }, Joined.with(
                        AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildDeleteUserRequest(envProps),
                        AvroSerdeFactory.buildUserEvent(envProps)));

        final String userValidationsTopic = envProps.getProperty("user_validation.topic.name");
        validatedRequests.to(userValidationsTopic,
                Produced.with(AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUserEvent(envProps)));
        return builder.build();
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("user_validation_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        return props;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }
        Properties envProps = loadEnvProperties(args[0]);
        ValidationService service = new ValidationService();
        Topology topology = service.buildTopology(envProps);
        log.debug("Built stream");

        Properties streamProps = service.buildStreamProperties(envProps);
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
