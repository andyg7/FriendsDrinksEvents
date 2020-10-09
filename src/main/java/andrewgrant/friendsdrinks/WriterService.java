package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

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
import andrewgrant.friendsdrinks.api.avro.EventType;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksIdList;

/**
 * Reads API results and writes to backend topics.
 */
public class WriterService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);


    public Topology buildTopology(Properties envProps, AvroBuilder avroBuilder,
                                  andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                  andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, FriendsDrinksEvent> apiEvents = builder.stream(envProps.getProperty("friendsdrinks-api.topic.name"),
                Consumed.with(Serdes.String(), apiAvroBuilder.friendsDrinksSerde()));
        KStream<String, FriendsDrinksEvent> successfulApiResponses = streamOfSuccessfulResponses(apiEvents);
        KStream<String, FriendsDrinksEvent> apiRequests = streamOfRequests(apiEvents);

        successfulApiResponses.join(apiRequests,
                (l, r) -> new RequestResponseJoiner().join(r),
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        apiAvroBuilder.friendsDrinksSerde(),
                        apiAvroBuilder.friendsDrinksSerde()))
                .selectKey((k, v) -> v.getFriendsDrinksId())
                .to(envProps.getProperty("friendsdrinks-event.topic.name"),
                        Produced.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksEventSerde()));

        streamFriendsDrinksMembershipEvents(builder, envProps, membershipAvroBuilder)
                .to(envProps.getProperty("friendsdrinks-event.topic.name"),
                        Produced.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksEventSerde()));

        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> friendsDrinksStateStream =
                builder.stream(envProps.getProperty("friendsdrinks-event.topic.name"),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksEventSerde()))
                        .groupByKey(Grouped.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksEventSerde()))
                        .aggregate(
                                () -> FriendsDrinksStateAggregate.newBuilder().build(),
                                (aggKey, newValue, aggValue) -> new StateAggregator().handleNewEvent(aggKey, newValue, aggValue),
                                Materialized.<
                                        andrewgrant.friendsdrinks.avro.FriendsDrinksId,
                                        FriendsDrinksStateAggregate, KeyValueStore<Bytes, byte[]>>
                                        as("internal_writer_service_friendsdrinks-state_tracker")
                                        .withKeySerde(avroBuilder.friendsDrinksIdSerde())
                                        .withValueSerde(avroBuilder.friendsDrinksStateAggregateSerde())
                        ).toStream().mapValues(value -> {
                    if (value == null) {
                        return null;
                    }
                    return value.getFriendsDrinksState();
                });

        friendsDrinksStateStream.to(envProps.getProperty("friendsdrinks-state.topic.name"),
                Produced.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));
        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable = builder.table(envProps.getProperty("friendsdrinks-state.topic.name"),
                Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));
        buildFriendsDrinksStateKeyedByAdminUserIdView(friendsDrinksStateKTable, envProps, avroBuilder);

        return builder.build();
    }

    private void buildFriendsDrinksStateKeyedByAdminUserIdView(KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                                               Properties envProps, AvroBuilder avroBuilder) {
        friendsDrinksStateKTable.groupBy(((key, value) ->
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
                .toStream().to(envProps.getProperty("friendsdrinks-keyed-by-admin-user-id-state"),
                Produced.with(Serdes.String(), avroBuilder.friendsDrinksIdListSerde()));

    }

    private KStream<String, FriendsDrinksEvent> streamOfSuccessfulResponses(KStream<String, FriendsDrinksEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_RESPONSE) &&
                        friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_RESPONSE) &&
                                friendsDrinksEvent.getUpdateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_RESPONSE) &&
                                friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS))
        );
    }

    private KStream<String, FriendsDrinksEvent> streamOfRequests(KStream<String, FriendsDrinksEvent> apiEvents) {
        return apiEvents.filter((k, v) -> v.getEventType().equals(
                EventType.CREATE_FRIENDSDRINKS_REQUEST) ||
                v.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_REQUEST) ||
                v.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_REQUEST));
    }

    private KStream<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> streamFriendsDrinksMembershipEvents(
            StreamsBuilder builder, Properties envProps,
            andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder) {

        return builder.stream(envProps.getProperty("friendsdrinks-membership-event.topic.name"),
                Consumed.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(), membershipAvroBuilder.friendsDrinksMembershipEventSerdes()))
                .map((key, value) -> {
                    FriendsDrinksId friendsDrinksId = FriendsDrinksId
                            .newBuilder()
                            .setUuid(value.getMembershipId().getFriendsDrinksId().getUuid())
                            .setAdminUserId(value.getMembershipId().getFriendsDrinksId().getAdminUserId())
                            .build();
                    andrewgrant.friendsdrinks.avro.FriendsDrinksEvent friendsDrinksEvent;
                    if (value.getEventType().equals(andrewgrant.friendsdrinks.membership.avro.EventType.USER_ADDED)) {
                        FriendsDrinksUserAdded friendsDrinksUserAdded = FriendsDrinksUserAdded
                                .newBuilder()
                                .setFriendsDrinksId(friendsDrinksId)
                                .setUserId(value.getMembershipId().getUserId().getUserId())
                                .build();
                        friendsDrinksEvent =
                                andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                                        .newBuilder()
                                        .setEventType(andrewgrant.friendsdrinks.avro.EventType.USER_ADDED)
                                        .setFriendsDrinksUserAdded(friendsDrinksUserAdded)
                                        .setFriendsDrinksId(friendsDrinksId)
                                        .build();
                    } else if (value.getEventType().equals(andrewgrant.friendsdrinks.membership.avro.EventType.USER_REMOVED)) {
                        FriendsDrinksUserRemoved friendsDrinksUserRemoved = FriendsDrinksUserRemoved
                                .newBuilder()
                                .setFriendsDrinksId(friendsDrinksId)
                                .setUserId(value.getMembershipId().getUserId().getUserId())
                                .build();
                        friendsDrinksEvent =
                                andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                                        .newBuilder()
                                        .setEventType(andrewgrant.friendsdrinks.avro.EventType.USER_REMOVED)
                                        .setFriendsDrinksUserRemoved(friendsDrinksUserRemoved)
                                        .setFriendsDrinksId(friendsDrinksId)
                                        .build();
                    } else {
                        throw new RuntimeException(String.format("Unknown event type %s", value.getEventType().name()));
                    }
                    return new KeyValue<>(friendsDrinksId, friendsDrinksEvent);
                });
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
        WriterService writerService = new WriterService();
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        AvroBuilder avroBuilder = new AvroBuilder(schemaRegistryUrl);
        Topology topology = writerService.buildTopology(envProps, avroBuilder,
                new andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder(schemaRegistryUrl),
                new andrewgrant.friendsdrinks.membership.AvroBuilder(schemaRegistryUrl));
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
