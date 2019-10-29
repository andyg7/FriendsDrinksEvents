package andrewgrant.friendsdrinks.fraud;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import andrewgrant.friendsdrinks.avro.UserId;
import andrewgrant.friendsdrinks.user.UserAvroSerdeFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Validates user request.
 */
public class ValidationService {

    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopic = envProps.getProperty("user.topic.name");
        KStream<UserId, User> userKStream = builder.stream(userTopic,
                Consumed.with(UserAvroSerdeFactory.buildUserId(envProps),
                        UserAvroSerdeFactory.buildUser(envProps)))
                .filter(((key, value) -> value.getEventType().equals(UserEvent.REQUESTED)));

        KGroupedStream<UserId, User> groupedStream = userKStream.groupByKey();

        SessionWindowedKStream<UserId, User> sessionWindowedKStream =
                groupedStream.windowedBy(SessionWindows.with(Duration.ofMinutes(5)));

        sessionWindowedKStream.aggregate(
                () -> 0L,
                ((key, value, aggregate) -> 1 + aggregate),
                ((aggKey, aggOne, aggTwo) -> aggOne + aggTwo),
                Materialized.with(null, Serdes.Long())
        )
                .toStream(((key, value) -> key.key()));

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");

        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        return props;
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        ValidationService validationService =
                new ValidationService();
        Properties envProps = validationService.loadEnvProperties(args[0]);
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

