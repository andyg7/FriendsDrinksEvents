package andrewgrant.friendsdrinks.user;

import static andrewgrant.friendsdrinks.env.Properties.load;

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

/**
 * Contains user service.
 */
public class Service {

    private static final Logger log = LoggerFactory.getLogger(Service.class);

    public Topology buildTopology(Properties envProps, UserAvroBuilder avro) {
        StreamsBuilder builder = new StreamsBuilder();

        final String apiTopicName = envProps.getProperty("user-event.topic.name");
        KStream<UserId, UserEvent> userEvents = builder.stream(apiTopicName,
                Consumed.with(avro.userIdSerde(), avro.userEventSerde()));
        userEvents
                .groupByKey(Grouped.with(avro.userIdSerde(), avro.userEventSerde()))
                .aggregate(
                        () -> UserStateAggregate.newBuilder().build(),
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getEventType().equals(EventType.LOGGED_IN)) {
                                UserStateAggregate userStateAggregate = UserStateAggregate
                                        .newBuilder(aggValue)
                                        .setUserState(UserState
                                                .newBuilder()
                                                .setUserId(newValue.getUserLoggedIn().getUserId())
                                                .setFirstName(newValue.getUserLoggedIn().getFirstName())
                                                .setLastName(newValue.getUserLoggedIn().getLastName())
                                                .setEmail(newValue.getUserLoggedIn().getEmail())
                                                .build())
                                        .build();
                                return userStateAggregate;
                            } else if (newValue.getEventType().equals(EventType.LOGGED_OUT)) {
                                return aggValue;
                            } else if (newValue.getEventType().equals(EventType.SIGNED_OUT_SESSION_EXPIRED)) {
                                return aggValue;
                            } else if (newValue.getEventType().equals(EventType.DELETED)) {
                                // Tombstone deleted user.
                                return null;
                            } else {
                                throw new RuntimeException(String.format("Unknown event type %s", newValue.getEventType().name()));
                            }
                        },
                        Materialized.with(avro.userIdSerde(), avro.userStateAggregateSerde())
                ).toStream().mapValues(value -> {
            if (value == null) {
                return null;
            } else {
                return value.getUserState();
            }
        })
                .to(envProps.getProperty("user-state.topic.name"), Produced.with(avro.userIdSerde(), avro.userStateSerde()));

        return builder.build();
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("user.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        Service service = new Service();
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        UserAvroBuilder userAvroBuilder = new UserAvroBuilder(schemaRegistryUrl);
        Topology topology = service.buildTopology(envProps, userAvroBuilder);
        Properties streamProps = service.buildStreamProperties(envProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);

        log.info("Starting Service application...");

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        kafkaStreams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
