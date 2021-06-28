package andrewgrant.friendsdrinks.meetup;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.streamsconfig.Config;

import com.sun.net.httpserver.HttpServer;

/**
 * Owns writing to meetup topics.
 */
public class WriterService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);

    private Properties envProps;
    private AvroBuilder avroBuilder;


    public WriterService(Properties envProps, AvroBuilder avroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<FriendsDrinksMeetupId, FriendsDrinksMeetupEvent> friendsDrinksMeetupEventKStream =
                builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEETUP_EVENT),
                        Consumed.with(avroBuilder.friendsDrinksMeetupIdSpecificAvroSerde(), avroBuilder.friendsDrinksMeetupEventSpecificAvroSerde()));
        friendsDrinksMeetupEventKStream.groupByKey(
                Grouped.with(avroBuilder.friendsDrinksMeetupIdSpecificAvroSerde(), avroBuilder.friendsDrinksMeetupEventSpecificAvroSerde()))
                .aggregate(
                        () -> FriendsDrinksMeetupStateAggregate.newBuilder().build(),
                        (aggKey, newValue, aggValue) -> {
                            switch (newValue.getEventType()) {
                                case SCHEDULED:
                                    FriendsDrinksMeetupState.Builder friendsDrinksMeetupStateBuilder
                                            = FriendsDrinksMeetupState.newBuilder();
                                    friendsDrinksMeetupStateBuilder.setMeetupId(newValue.getMeetupScheduled().getMeetupId());
                                    friendsDrinksMeetupStateBuilder.setFriendsDrinksId(newValue.getMeetupScheduled().getFriendsDrinksId());
                                    friendsDrinksMeetupStateBuilder.setUserIds(newValue.getMeetupScheduled().getUserIds());
                                    friendsDrinksMeetupStateBuilder.setStatus(FriendsDrinksMeetupStatus.SCHEDULED);
                                    friendsDrinksMeetupStateBuilder.setDate(newValue.getMeetupScheduled().getDate());
                                    return FriendsDrinksMeetupStateAggregate
                                            .newBuilder(aggValue)
                                            .setFriendsDrinksMeetupState(friendsDrinksMeetupStateBuilder.build())
                                            .build();
                                case HAPPENED:
                                    FriendsDrinksMeetupState friendsDrinksMeetupState = aggValue.getFriendsDrinksMeetupState();
                                    friendsDrinksMeetupState.setStatus(FriendsDrinksMeetupStatus.HAPPENED);
                                    return FriendsDrinksMeetupStateAggregate
                                            .newBuilder(aggValue)
                                            .setFriendsDrinksMeetupState(friendsDrinksMeetupState)
                                            .build();
                                default:
                                    throw new RuntimeException(
                                            String.format("Unknown event type %s", newValue.getEventType().name()));
                            }
                        },
                        Materialized.with(
                                avroBuilder.friendsDrinksMeetupIdSpecificAvroSerde(),
                                avroBuilder.friendsDrinksMeetupStateAggregateSpecificAvroSerde())
                ).toStream().mapValues(v -> {
            if (v == null) {
                return null;
            } else {
                return v.getFriendsDrinksMeetupState();
            }
        }).to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEETUP_STATE),
                Produced.with(avroBuilder.friendsDrinksMeetupIdSpecificAvroSerde(), avroBuilder.friendsDrinksMeetupStateSpecificAvroSerde()));
        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-meetup-writer.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        if (envProps.getProperty("streams.dir") != null) {
            streamProps.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("streams.dir"));
        }
        streamProps = Config.addSharedConfig(streamProps);
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");

        WriterService writerService = new WriterService(envProps, new AvroBuilder(schemaRegistryUrl));

        Topology topology = writerService.buildTopology();
        Properties streamProps = writerService.buildStreamsProperties(envProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);
        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception {}", exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        log.info("Starting WriterService application...");

        HttpServer healthCheckServer = andrewgrant.friendsdrinks.health.Server.buildServer(8080, kafkaStreams);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                log.info("Running shutdown hook...");
                andrewgrant.friendsdrinks.health.Server.stop(healthCheckServer);
                kafkaStreams.close();
                latch.countDown();
            }
        });

        andrewgrant.friendsdrinks.health.Server.start(healthCheckServer);
        kafkaStreams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
