package andrewgrant.friendsdrinks.user;

import static andrewgrant.friendsdrinks.env.Properties.load;

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

import andrewgrant.friendsdrinks.user.api.avro.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;


/**
 * Aggregates create user validations and returns final result to orders topic.
 */
public class CreateUserValidationAggregatorService {

    private static final Logger log = LoggerFactory
            .getLogger(CreateUserValidationAggregatorService.class);

    public Topology buildTopology(Properties envProps,
                                  UserAvro userAvro) {
        StreamsBuilder builder = new StreamsBuilder();
        final String userValidationsTopic = envProps.getProperty("userValidation.topic.name");

        SpecificAvroSerde<UserEvent> userEventSerde = userAvro.userEventSerde();
        KStream<UserId, UserEvent> userValidations = builder
                .stream(userValidationsTopic, userAvro.consumedWith());

        // Re-key by request id.
        KStream<String, UserEvent> validationResultsKeyedByRequestId = userValidations
                .filter(((key, value) ->
                        value.getEventType().equals(EventType.CREATE_USER_VALIDATED) ||
                                value.getEventType().equals(EventType.CREATE_USER_REJECTED)))
                .selectKey((key, value) -> {
                    EventType eventType = value.getEventType();
                    if (eventType.equals(EventType.CREATE_USER_VALIDATED)) {
                        return value.getCreateUserValidated().getRequestId();
                    } else if (eventType.equals(EventType.CREATE_USER_REJECTED)) {
                        return value.getCreateUserRejected().getRequestId();
                    } else {
                        throw new RuntimeException(
                                String.format("Topic should only contain validated or rejected " +
                                        "events, but found %s", value.getEventType().toString()));
                    }
                });

        // Request id -> number of validations for create user requests
        KStream<String, Long> createValidationCount = validationResultsKeyedByRequestId
                .groupByKey(Grouped.with(Serdes.String(), userEventSerde))
                .windowedBy(SessionWindows.with(Duration.ofSeconds(1)))
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
                .filter(((key, value) -> value >= 1L));

        final String userAPITopicName = envProps.getProperty("user_api.topic.name");
        KStream<UserId, UserEvent> userEvents = builder.stream(
                userAPITopicName, userAvro.consumedWith());

        KStream<String, CreateUserRequest> createUserRequestsKeyedByRequestId = userEvents
                .filter(((key, value) ->
                        value.getEventType().equals(EventType.CREATE_USER_REQUEST)))
                .mapValues(value -> value.getCreateUserRequest())
                .selectKey((key, value) -> value.getRequestId())
                .through((String) envProps.get("userAggregationPrivate.topic.name"),
                        Produced.with(Serdes.String(), userAvro.createUserRequestSerde()));

        SpecificAvroSerde<CreateUserRequest> createUserRequestSerde = userAvro.createUserRequestSerde();
        createValidationCount.join(createUserRequestsKeyedByRequestId,
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
                JoinWindows.of(Duration.ofSeconds(10)),
                Joined.with(Serdes.String(), Serdes.Long(), createUserRequestSerde))
                .selectKey(((key, value) -> value.getCreateUserResponse().getUserId()))
                .to(userAPITopicName, userAvro.producedWith());


        // Rejected create user requests
        validationResultsKeyedByRequestId.filter(((key, value) ->
                value.getEventType().equals(EventType.CREATE_USER_REJECTED))).join(
                createUserRequestsKeyedByRequestId,
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
                JoinWindows.of(Duration.ofSeconds(10)),
                Joined.with(Serdes.String(), userEventSerde, createUserRequestSerde))
                .groupByKey(Grouped.with(Serdes.String(), userEventSerde))
                .reduce((key, value) -> value)
                .toStream()
                .selectKey(((key, value) -> value.getCreateUserResponse().getUserId()))
                .to(userAPITopicName, userAvro.producedWith());

        return builder.build();
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("create_user_validation_aggregator_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        // Disable caching.
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return props;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }
        Properties envProps = load(args[0]);
        CreateUserValidationAggregatorService service = new CreateUserValidationAggregatorService();
        UserAvro userAvro = new UserAvro(envProps.getProperty("schema.registry.url"));
        Topology topology = service.buildTopology(envProps, userAvro);
        log.debug("Built stream");
        log.info("Topology description:\n {}", topology.describe());

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

