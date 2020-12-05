package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.*;

/**
 * Reads API results and writes to backend topics.
 */
public class WriterService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);

    private Properties envProps;
    private AvroBuilder avroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder;

    public WriterService(Properties envProps, AvroBuilder avroBuilder,
                         andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
        this.frontendAvroBuilder = frontendAvroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ApiEvent> apiEvents = builder.stream(envProps.getProperty(FRIENDSDRINKS_API),
                Consumed.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));
        KStream<String, FriendsDrinksApiEvent> successfulApiResponses = streamOfSuccessfulResponses(apiEvents);
        KStream<String, FriendsDrinksApiEvent> apiRequests = streamOfRequests(apiEvents);

        successfulApiResponses.join(apiRequests,
                (l, r) -> new RequestResponseJoiner().join(r),
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        frontendAvroBuilder.friendsDrinksApiEventSerde(),
                        frontendAvroBuilder.friendsDrinksApiEventSerde()))
                .selectKey((k, v) -> v.getFriendsDrinksId())
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_EVENT),
                        Produced.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksEventSerde()));

        builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_EVENT),
                Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksEventSerde()))
                .groupByKey(Grouped.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksEventSerde()))
                .aggregate(
                        () -> FriendsDrinksStateAggregate.newBuilder().build(),
                        (aggKey, newValue, aggValue) -> new StateAggregator().handleNewEvent(aggKey, newValue, aggValue),
                        Materialized.<
                                andrewgrant.friendsdrinks.avro.FriendsDrinksId,
                                FriendsDrinksStateAggregate, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-state-aggregate-state-store")
                                .withKeySerde(avroBuilder.friendsDrinksIdSerde())
                                .withValueSerde(avroBuilder.friendsDrinksStateAggregateSerde())
                ).toStream().mapValues(value -> {
            if (value == null) {
                return null;
            }
            return value.getFriendsDrinksState();
        }).to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_STATE),
                Produced.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        return builder.build();
    }

    private KStream<String, FriendsDrinksApiEvent> streamOfSuccessfulResponses(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((k, v) -> v.getEventType().equals(ApiEventType.FRIENDSDRINKS_EVENT))
        .filter((friendsDrinksId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getFriendsDrinksEvent().getEventType().equals(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_RESPONSE) &&
                        friendsDrinksEvent.getFriendsDrinksEvent().getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getFriendsDrinksEvent().getEventType().equals(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_RESPONSE) &&
                                friendsDrinksEvent.getFriendsDrinksEvent().getUpdateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getFriendsDrinksEvent().getEventType().equals(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_RESPONSE) &&
                                friendsDrinksEvent.getFriendsDrinksEvent().getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS))
        ).mapValues(v -> v.getFriendsDrinksEvent());
    }

    private KStream<String, FriendsDrinksApiEvent> streamOfRequests(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((k, v) -> v.getEventType().equals(ApiEventType.FRIENDSDRINKS_EVENT))
                .filter((k, v) -> v.getFriendsDrinksEvent().getEventType().equals(
                        FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_REQUEST) ||
                        v.getFriendsDrinksEvent().getEventType().equals(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_REQUEST) ||
                        v.getFriendsDrinksEvent().getEventType().equals(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_REQUEST))
                .mapValues(v -> v.getFriendsDrinksEvent());
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-writer.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        WriterService writerService = new WriterService(
                envProps,
                new AvroBuilder(schemaRegistryUrl),
                new andrewgrant.friendsdrinks.frontend.AvroBuilder(schemaRegistryUrl));
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
