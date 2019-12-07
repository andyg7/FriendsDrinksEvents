package andrewgrant.friendsdrinks.user;

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

import andrewgrant.friendsdrinks.avro.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;


/**
 * Aggregates validations and returns final result to orders topic.
 */
public class ValidationAggregatorService {

    private static final Logger log = LoggerFactory.getLogger(ValidationAggregatorService.class);

    public Topology buildTopology(Properties envProps) {
        StreamsBuilder builder = new StreamsBuilder();
        final String userValidationsTopic = envProps.getProperty("user_validation.topic.name");

        SpecificAvroSerde<UserId> userIdSerde = AvroSerdeFactory.buildUserId(envProps);
        SpecificAvroSerde<UserEvent> userEventSerde = AvroSerdeFactory.buildUserEvent(envProps);
        KStream<UserId, UserEvent> userValidations = builder
                .stream(userValidationsTopic, Consumed.with(
                        userIdSerde, userEventSerde));

        // Re-key by request id.
        KStream<String, UserEvent> validationResultsKeyedByRequestId = userValidations
                .selectKey((key, value) -> {
                    if (value.getEventType().equals(EventType.VALIDATED)) {
                        return value.getUserValidated().getRequestId();
                    } else if (value.getEventType().equals(EventType.REJECTED)) {
                        return value.getUserRejected().getRequestId();
                    } else {
                        throw new RuntimeException(
                                String.format("Topic should only contain validated or rejected " +
                                        "events, but found %s", value.getEventType().toString()));
                    }
                });

        // Request id -> number of validations
        Grouped<String, UserEvent> groupedSerdes = Grouped.with(Serdes.String(), userEventSerde);
        KStream<String, Long> validationCount = validationResultsKeyedByRequestId
                .groupByKey(groupedSerdes)
                .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                .aggregate(
                        () -> 0L,
                        (requestId, user, total) ->
                                user.getEventType().equals(EventType.VALIDATED) ?
                                        Long.valueOf(total + 1L) : total,
                        (k, a, b) -> b == null ? a : b,
                        Materialized.with(null, Serdes.Long())
                )
                .toStream((key, value) -> key.key())
                .filter(((key, value) -> value != null))
                .filter(((key, value) -> value >= 2L));

        KStream<String, UserValidation> usersWithValidationCount =
                validationResultsKeyedByRequestId.join(validationCount,
                        (leftValue, rightValue) -> {
                            if (leftValue.getEventType().equals(EventType.VALIDATED)) {
                                UserValidated userValidated = leftValue.getUserValidated();
                                User user = new User(userValidated.getUserId(),
                                        userValidated.getEmail(),
                                        userValidated.getRequestId());
                                return new UserValidation(user, rightValue, 2L);
                            } else if (leftValue.getEventType().equals(EventType.REJECTED)) {
                                UserRejected userRejected = leftValue.getUserRejected();
                                User user = new User(userRejected.getUserId(),
                                        userRejected.getEmail(),
                                        userRejected.getRequestId());
                                return new UserValidation(user, rightValue, 2L);
                            } else {
                                throw new RuntimeException(
                                        String.format("Received unknown event type %s",
                                                leftValue.getEventType().toString()));
                            }
                        },
                        JoinWindows.of(Duration.ofMinutes(5)),
                        Joined.with(Serdes.String(), userEventSerde, Serdes.Long()));


        KStream<String, UserValidation>[] results = usersWithValidationCount.branch(
                ((key, value) -> value.isValidated()),
                ((key, value) -> true)
        );

        final String usersTopic = envProps.getProperty("user.topic.name");
        results[0].selectKey(((key, value) -> value.getUser().getUserId()))
                .mapValues(user -> {
                    UserValidated userValidated = UserValidated
                            .newBuilder()
                            .setUserId(user.getUser().getUserId())
                            .setRequestId(user.getUser().getRequestId())
                            .setEmail(user.getUser().getEmail())
                            .build();
                    return UserEvent.newBuilder()
                            .setEventType(EventType.VALIDATED)
                            .setUserValidated(userValidated)
                            .build();
                })
                .to(usersTopic, Produced.with(userIdSerde, userEventSerde));

        results[1].selectKey(((key, value) -> value.getUser().getUserId()))
                .mapValues(user -> {
                    UserRejected userRejected = UserRejected
                            .newBuilder()
                            .setUserId(user.getUser().getUserId())
                            .setRequestId(user.getUser().getRequestId())
                            .setEmail(user.getUser().getEmail())
                            .setErrorCode(ErrorCode.InvalidRequest.toString())
                            .build();
                    return UserEvent.newBuilder()
                            .setEventType(EventType.REJECTED)
                            .setUserRejected(userRejected)
                            .build();
                })
                .to(usersTopic, Produced.with(userIdSerde, userEventSerde));

        return builder.build();
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("user_validation_aggregator_application.id"));
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
        ValidationAggregatorService service = new ValidationAggregatorService();
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

