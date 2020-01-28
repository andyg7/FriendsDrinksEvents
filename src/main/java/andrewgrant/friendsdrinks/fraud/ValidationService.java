package andrewgrant.friendsdrinks.fraud;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;


/**
 * Validates user request.
 */
public class ValidationService {

    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);

    public Topology buildTopology(Properties envProps,
                                  UserAvro userAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopicName = envProps.getProperty("user.topic.name");
        final String fraudPrivateTopic = envProps.getProperty("fraudPrivate.topic.name");
        KStream<UserId, UserEvent> users = builder.stream(userTopicName, userAvro.consumedWith());

        users.filter(((key, value) -> value.getEventType().equals(EventType.CREATE_USER_REQUEST)))
                .groupByKey(Grouped.with(userAvro.userIdSerde(),
                        userAvro.userEventSerde()))
                .windowedBy(SessionWindows.with(Duration.ofMinutes(1)))
                .aggregate(
                        () -> 0L,
                        ((key, value, aggregate) -> 1 + aggregate),
                        ((aggKey, aggOne, aggTwo) -> aggOne + aggTwo),
                        Materialized.with(userAvro.userIdSerde(), Serdes.Long()))
                // Get rid of windowed key.
                .toStream(((key, value) -> key.key()))
                .to(fraudPrivateTopic,
                        Produced.with(userAvro.userIdSerde(),
                                Serdes.Long()));

        KTable<UserId, Long> userRequestCount = builder.table(fraudPrivateTopic,
                Consumed.with(userAvro.userIdSerde(), Serdes.Long()));

        KStream<UserId, CreateUserRequest> userRequests = users.filter((
                (key, value) -> value.getEventType().equals(EventType.CREATE_USER_REQUEST)))
                .mapValues(value -> value.getCreateUserRequest());

        KStream<UserId, Tracker> trackedUsers = userRequests
                .leftJoin(userRequestCount,
                        Tracker::new,
                        Joined.with(userAvro.userIdSerde(),
                        userAvro.createUserRequestSerde(),
                        Serdes.Long()));

        KStream<UserId, Tracker>[] trackedUserResults = trackedUsers.branch(
                (key, tracker) -> tracker.getCount() == null ||
                        tracker.getCount() < 10,
                (key, value) -> true);

        final String userValidationsTopic = envProps.getProperty("userValidation.topic.name");

        // Validated requests.
        trackedUserResults[0].mapValues(value -> value.getRequest())
                .mapValues(value -> {
                    CreateUserValidated userValidated = CreateUserValidated.newBuilder()
                            .setEmail(value.getEmail())
                            .setUserId(value.getUserId())
                            .setRequestId(value.getRequestId())
                            .setSource("fraud")
                            .build();
                    return UserEvent.newBuilder()
                            .setEventType(EventType.CREATE_USER_VALIDATED)
                            .setCreateUserValidated(userValidated)
                            .build();
                })
                .to(userValidationsTopic, userAvro.producedWith());

        // Rejected requests.
        trackedUserResults[1].mapValues(value -> value.getRequest())
                .mapValues(value -> {
                    CreateUserRejected userRejected = CreateUserRejected.newBuilder()
                            .setEmail(value.getEmail())
                            .setUserId(value.getUserId())
                            .setRequestId(value.getRequestId())
                            .setErrorCode(ErrorCode.TooManyRequests.toString())
                            .build();
                    return UserEvent.newBuilder()
                            .setEventType(EventType.CREATE_USER_REJECTED)
                            .setCreateUserRejected(userRejected)
                            .build();
                })
                .to(userValidationsTopic, userAvro.producedWith());

        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("fraud_validation_application.id"));
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

        ValidationService validationService =
                new ValidationService();
        Properties envProps = loadEnvProperties(args[0]);
        UserAvro userAvro = new UserAvro(
                envProps.getProperty("schema.registry.url"));
        Topology topology = validationService.buildTopology(envProps,
                userAvro);
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

