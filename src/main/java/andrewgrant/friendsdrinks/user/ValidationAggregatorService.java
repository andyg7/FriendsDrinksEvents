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
                    if (value.getEventType().equals(EventType.CREATE_USER_VALIDATED)) {
                        return value.getCreateUserValidated().getRequestId();
                    } else if (value.getEventType().equals(EventType.CREATE_USER_REJECTED)) {
                        return value.getCreateUserRejected().getRequestId();
                    } else {
                        throw new RuntimeException(
                                String.format("Topic should only contain validated or rejected " +
                                        "events, but found %s", value.getEventType().toString()));
                    }
                });

        // Request id -> number of validations
        KStream<String, Long> validationCount = validationResultsKeyedByRequestId
                .groupByKey(Grouped.with(Serdes.String(), userEventSerde))
                .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                .aggregate(
                        () -> 0L,
                        (requestId, user, total) ->
                                user.getEventType().equals(EventType.CREATE_USER_VALIDATED) ?
                                        Long.valueOf(total + 1L) : total,
                        (k, a, b) -> b == null ? a : b,
                        Materialized.with(null, Serdes.Long())
                )
                .toStream((key, value) -> key.key())
                .filter(((key, value) -> value != null))
                .filter(((key, value) -> value >= 2L));

        final String userTopic = envProps.getProperty("user.topic.name");
        KStream<UserId, UserEvent> userEvents = builder.stream(
                userTopic, Consumed.with(userIdSerde, userEventSerde));

        KStream<String, CreateUserRequest> userRequestsKeyedByRequestId = userEvents
                .filter(((key, value) ->
                        value.getEventType().equals(EventType.CREATE_USER_REQUEST)))
                .mapValues(value -> value.getCreateUserRequest())
                .selectKey((key, value) -> value.getRequestId());

        KStream<String, CreateUserRequest> userRequestsKeyedByRequestId2 = userEvents
                .filter(((key, value) ->
                        value.getEventType().equals(EventType.CREATE_USER_REQUEST)))
                .mapValues(value -> value.getCreateUserRequest())
                .selectKey((key, value) -> value.getRequestId());

        SpecificAvroSerde<CreateUserRequest> userRequestSerde =
                AvroSerdeFactory.buildCreateUserRequest(envProps);
        validationCount.join(userRequestsKeyedByRequestId,
                (leftValue, rightValue) -> {
                    CreateUserResponse response = CreateUserResponse.newBuilder()
                            .setRequestId(rightValue.getRequestId())
                            .setUserId(rightValue.getUserId())
                            .setResult(Result.SUCCESS)
                            .build();
                    return UserEvent.newBuilder()
                            .setEventType(EventType.CREATE_USER_RESPONSE)
                            .setCreateUserResponse(response)
                            .build();
                },
                JoinWindows.of(Duration.ofMinutes(5)),
                Joined.with(Serdes.String(), Serdes.Long(), userRequestSerde))
                .selectKey(((key, value) -> value.getCreateUserResponse().getUserId()))
                .to(userTopic, Produced.with(userIdSerde, userEventSerde));

        validationResultsKeyedByRequestId.filter(((key, value) ->
                value.getEventType().equals(EventType.CREATE_USER_REJECTED))).join(
                userRequestsKeyedByRequestId2,
                (leftValue, rightValue) -> {
                    CreateUserResponse response = CreateUserResponse.newBuilder()
                            .setRequestId(rightValue.getRequestId())
                            .setUserId(rightValue.getUserId())
                            .setResult(Result.FAIL)
                            .build();
                    return UserEvent.newBuilder()
                            .setEventType(EventType.CREATE_USER_RESPONSE)
                            .setCreateUserResponse(response)
                            .build();
                },
                JoinWindows.of(Duration.ofMinutes(5)),
                Joined.with(Serdes.String(), userEventSerde, userRequestSerde))
                .groupByKey(Grouped.with(Serdes.String(), userEventSerde))
                .reduce((key, value) -> value)
                .toStream()
                .selectKey(((key, value) -> value.getCreateUserResponse().getUserId()))
                .to(userTopic, Produced.with(userIdSerde, userEventSerde));

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

