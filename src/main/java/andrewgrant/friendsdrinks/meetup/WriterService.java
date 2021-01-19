package andrewgrant.friendsdrinks.meetup;

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

import andrewgrant.friendsdrinks.avro.FriendsDrinksMeetupEvent;
import andrewgrant.friendsdrinks.avro.FriendsDrinksMeetupId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksMeetupState;
import andrewgrant.friendsdrinks.avro.FriendsDrinksMeetupStatus;

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
                        () -> FriendsDrinksMeetupState.newBuilder().build(),
                        (aggKey, newValue, aggValue) -> {
                            switch (newValue.getEventType()) {
                                case SCHEDULED:
                                    aggValue.setMeetupId(newValue.getMeetupScheduled().getMeetupId());
                                    aggValue.setFriendsDrinksId(newValue.getMeetupScheduled().getFriendsDrinksId());
                                    aggValue.setUserIds(newValue.getMeetupScheduled().getUserIds());
                                    aggValue.setStatus(FriendsDrinksMeetupStatus.SCHEDULED);
                                    aggValue.setDate(newValue.getMeetupScheduled().getDate());
                                    return aggValue;
                                case HAPPENED:
                                    aggValue.setStatus(FriendsDrinksMeetupStatus.HAPPENED);
                                    return aggValue;
                                default:
                                    throw new RuntimeException(
                                            String.format("Unknown event type %s", newValue.getEventType().name()));
                            }
                        },
                        Materialized.with(
                                avroBuilder.friendsDrinksMeetupIdSpecificAvroSerde(),
                                avroBuilder.friendsDrinksMeetupStateSpecificAvroSerde())
                ).toStream().to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEETUP_STATE),
                Produced.with(avroBuilder.friendsDrinksMeetupIdSpecificAvroSerde(), avroBuilder.friendsDrinksMeetupStateSpecificAvroSerde()));
        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-meetup-writer.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");

        WriterService writerService = new WriterService(envProps, new AvroBuilder(schemaRegistryUrl));

        Topology topology = writerService.buildTopology();
        Properties streamProps = writerService.buildStreamsProperties(envProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);
        log.info("Starting WriterService application...");

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
