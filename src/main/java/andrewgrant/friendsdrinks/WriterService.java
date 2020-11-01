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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.api.avro.ApiEvent;
import andrewgrant.friendsdrinks.api.avro.EventType;
import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksIdList;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;

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
                Consumed.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));
        KStream<String, ApiEvent> successfulApiResponses = streamOfSuccessfulResponses(apiEvents);
        KStream<String, ApiEvent> apiRequests = streamOfRequests(apiEvents);

        successfulApiResponses.join(apiRequests,
                (l, r) -> new RequestResponseJoiner().join(r),
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        frontendAvroBuilder.friendsDrinksSerde(),
                        frontendAvroBuilder.friendsDrinksSerde()))
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
                                as("friendsdrinks-aggregate-state-store")
                                .withKeySerde(avroBuilder.friendsDrinksIdSerde())
                                .withValueSerde(avroBuilder.friendsDrinksStateAggregateSerde())
                ).toStream().mapValues(value -> {
            if (value == null) {
                return null;
            }
            return value.getFriendsDrinksState();
        }).to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_STATE),
                Produced.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_STATE),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));
        buildFriendsDrinksIdListKeyedByAdminUserIdView(friendsDrinksStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_KEYED_BY_ADMIN_USER_ID_STATE),
                        Produced.with(Serdes.String(), avroBuilder.friendsDrinksIdListSerde()));

        return builder.build();
    }

    private KStream<String, FriendsDrinksIdList> buildFriendsDrinksIdListKeyedByAdminUserIdView(
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {
        return friendsDrinksStateKTable.groupBy(((key, value) ->
                        KeyValue.pair(value.getFriendsDrinksId().getAdminUserId(), value)),
                Grouped.with(Serdes.String(), avroBuilder.friendsDrinksStateSerde()))
                .aggregate(
                        () -> FriendsDrinksIdList.newBuilder().setIds(new ArrayList<>()).build(),
                        (aggKey, newValue, aggValue) -> {
                            List<FriendsDrinksId> ids = aggValue.getIds();
                            ids.add(newValue.getFriendsDrinksId());
                            FriendsDrinksIdList idList = FriendsDrinksIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<FriendsDrinksId> ids = aggValue.getIds();
                            for (int i = 0; i < ids.size(); i++) {
                                if (ids.get(i).equals(oldValue.getFriendsDrinksId())) {
                                    ids.remove(i);
                                    break;
                                }
                            }
                            if (ids.size() == 0) {
                                return null;
                            }
                            FriendsDrinksIdList idList = FriendsDrinksIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        Materialized.<String, FriendsDrinksIdList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-keyed-by-admin-user-id-state-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(avroBuilder.friendsDrinksIdListSerde())
                )
                .toStream();
    }

    private KStream<String, ApiEvent> streamOfSuccessfulResponses(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_RESPONSE) &&
                        friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_RESPONSE) &&
                                friendsDrinksEvent.getUpdateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_RESPONSE) &&
                                friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS))
        );
    }

    private KStream<String, ApiEvent> streamOfRequests(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((k, v) -> v.getEventType().equals(
                EventType.CREATE_FRIENDSDRINKS_REQUEST) ||
                v.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_REQUEST) ||
                v.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_REQUEST));
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
