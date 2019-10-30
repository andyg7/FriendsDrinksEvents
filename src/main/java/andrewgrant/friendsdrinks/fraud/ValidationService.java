package andrewgrant.friendsdrinks.fraud;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import andrewgrant.friendsdrinks.avro.UserId;
import andrewgrant.friendsdrinks.user.AvroSerdeFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;


/**
 * Validates user request.
 */
public class ValidationService {

    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);

    public static final String PROCESSING_USERS_STORE_NAME =
            "processing_users";

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder processingUsers = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(PROCESSING_USERS_STORE_NAME),
                        AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUser(envProps))
                .withLoggingEnabled(new HashMap<>());

        builder.addStateStore(processingUsers);

        final String userTopic = envProps.getProperty("user.topic.name");
        KStream<UserId, User> userKStream = builder.stream(userTopic,
                Consumed.with(AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUser(envProps)))
                .filter(((key, value) -> value.getEventType().equals(UserEvent.REQUESTED)));

        userKStream = userKStream.transform(
                ProcessingUsersPopulator::new,
                PROCESSING_USERS_STORE_NAME);

        KGroupedStream<UserId, User> groupedStream = userKStream.groupByKey(
                Grouped.with(AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUser(envProps)));

        SessionWindowedKStream<UserId, User> sessionWindowedKStream =
                groupedStream.windowedBy(SessionWindows.with(Duration.ofMinutes(1)));

        KStream<UserId, Long> requestCounts = sessionWindowedKStream.aggregate(
                () -> 0L,
                ((key, value, aggregate) -> 1 + aggregate),
                ((aggKey, aggOne, aggTwo) -> aggOne + aggTwo),
                Materialized.with(AvroSerdeFactory.buildUserId(envProps), Serdes.Long())
        ).toStream(((key, value) -> key.key()));

        KStream<UserId, User> userValidatedStream = requestCounts
                .transform(ProcesssingUsersCleaner::new, PROCESSING_USERS_STORE_NAME);

        final String userValidationsTopic = envProps.getProperty("user_validation.topic.name");
        userValidatedStream.to(userValidationsTopic, Produced.with(
                AvroSerdeFactory.buildUserId(envProps),
                AvroSerdeFactory.buildUser(envProps)));

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

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        ValidationService validationService =
                new ValidationService();
        Properties envProps = loadEnvProperties(args[0]);
        Topology topology = validationService.buildTopology(envProps);
        log.debug("Built stream");

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

