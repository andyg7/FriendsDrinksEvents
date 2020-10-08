package andrewgrant.friendsdrinks.membership;

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
import andrewgrant.friendsdrinks.membership.avro.*;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.membership.avro.UserId;

/**
 * Owns writing to friendsdrinks-membership-event.
 */
public class WriterService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);

    public Topology buildTopology(Properties envProps, AvroBuilder avroBuilder,
                                  andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                  andrewgrant.friendsdrinks.AvroBuilder friendsDrinksAvroBuilder) {
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
                .selectKey((k, v) -> v.getMembershipId())
                .to(envProps.getProperty("friendsdrinks-membership-event.topic.name"),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));

        KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> friendsDrinksMembershipEventKStream
                = builder.stream(envProps.getProperty("friendsdrinks-membership-event.topic.name"),
                Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));
        buildMembershipStateKTable(friendsDrinksMembershipEventKStream, envProps, avroBuilder);

        KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKTable =
                builder.table(envProps.getProperty("friendsdrinks-membership-state.topic.name"),
                        Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()));
        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState>
                friendsDrinksEventKStream = builder.stream(envProps.getProperty("friendsdrinks-state.topic.name"),
                Consumed.with(friendsDrinksAvroBuilder.friendsDrinksIdSerde(), friendsDrinksAvroBuilder.friendsDrinksStateSerde()));
        handleDeletedFriendsDrinks(friendsDrinksEventKStream, membershipStateKTable, envProps, friendsDrinksAvroBuilder, avroBuilder);

        return builder.build();
    }

    private void handleDeletedFriendsDrinks(KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState>
                                                    friendsDrinksEventKStream,
                                            KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> friendsDrinksMembershipStateKTable,
                                            Properties envProp,
                                            andrewgrant.friendsdrinks.AvroBuilder friendsDrinksAvroBuilder,
                                            AvroBuilder avroBuilder) {

        KTable<FriendsDrinksId, UserIdList> userIdListKTable = friendsDrinksMembershipStateKTable.groupBy((key, value) ->
                        KeyValue.pair(value.getMembershipId().getFriendsDrinksId(), value),
                Grouped.with(avroBuilder.friendsDrinksIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()))
                .aggregate(
                        () -> UserIdList
                                .newBuilder()
                                .setIds(new ArrayList<>())
                                .build(),
                        (aggKey, newValue, aggValue) -> {
                            List<UserId> ids = aggValue.getIds();
                            ids.add(newValue.getMembershipId().getUserId());
                            UserIdList userIdList = UserIdList.newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return userIdList;
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<UserId> ids = aggValue.getIds();
                            for (int i = 0; i < ids.size(); i++) {
                                if (ids.get(i).getUserId().equals(oldValue.getMembershipId().getUserId().getUserId())) {
                                    ids.remove(i);
                                    break;
                                }
                            }
                            UserIdList userIdList = UserIdList.newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return userIdList;
                        },
                        Materialized.<FriendsDrinksId, UserIdList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-user-id-list-state-store")
                                .withKeySerde(avroBuilder.friendsDrinksIdSerdes())
                                .withValueSerde(avroBuilder.userIdListSerdes())
                );

        KStream<FriendsDrinksId, UserId> streamOfUserIdsToDelete = friendsDrinksEventKStream
                .map((key, value) -> {
                    FriendsDrinksId friendsDrinksId = FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(key.getAdminUserId())
                            .setUuid(key.getUuid())
                            .build();
                    if (value == null) {
                        return KeyValue.pair(friendsDrinksId, friendsDrinksId);
                    } else {
                        return KeyValue.pair(friendsDrinksId, null);
                    }
                })
                .filter((key, value) -> value != null)
                .leftJoin(userIdListKTable,
                        (l, r) -> r,
                        Joined.with(
                                avroBuilder.friendsDrinksIdSerdes(),
                                avroBuilder.friendsDrinksIdSerdes(),
                                avroBuilder.userIdListSerdes())
                )
                .filter((key, value) -> value != null)
                .flatMapValues(value -> value.getIds());

        streamOfUserIdsToDelete.map((key, value) -> {
            FriendsDrinksMembershipId friendsDrinksMembershipId = FriendsDrinksMembershipId
                    .newBuilder()
                    .setUserId(UserId.newBuilder().setUserId(value.getUserId()).build())
                    .setFriendsDrinksId(
                            FriendsDrinksId
                                    .newBuilder()
                                    .setAdminUserId(key.getAdminUserId())
                                    .setUuid(key.getUuid())
                                    .build())
                    .build();
            FriendsDrinksMembershipEvent event = FriendsDrinksMembershipEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.membership.avro.EventType.USER_REMOVED)
                    .setMembershipId(friendsDrinksMembershipId)
                    .setFriendsDrinksUserRemoved(FriendsDrinksUserRemoved
                            .newBuilder()
                            .setMembershipId(friendsDrinksMembershipId)
                            .build())
                    .build();
            return KeyValue.pair(friendsDrinksMembershipId, event);
        })
                .to(envProp.getProperty("friendsdrinks-membership-event.topic.name"),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));


    }

    private void buildMembershipStateKTable(KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> membershipEventKStream,
                                            Properties envProps, AvroBuilder avroBuilder) {
        membershipEventKStream.mapValues(value -> {
            if (value.getEventType().equals(andrewgrant.friendsdrinks.membership.avro.EventType.USER_REMOVED)) {
                return null;
            } else {
                return FriendsDrinksMembershipState.newBuilder()
                        .setMembershipId(value.getMembershipId())
                        .setStatus(Status.ACTIVE)
                        .build();
            }
        }).to(envProps.getProperty("friendsdrinks-membership-state.topic.name"),
                Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()));
    }

    private KStream<String, FriendsDrinksEvent> streamOfSuccessfulResponses(KStream<String, FriendsDrinksEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getEventType().equals(EventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) &&
                        friendsDrinksEvent.getFriendsDrinksInvitationReplyResponse().getResult().equals(Result.SUCCESS))
        );
    }

    private KStream<String, FriendsDrinksEvent> streamOfRequests(KStream<String, FriendsDrinksEvent> apiEvents) {
        return apiEvents.filter((k, v) -> (v.getEventType().equals(EventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                && v.getFriendsDrinksInvitationReplyRequest().getReply().equals(Reply.ACCEPTED));
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("friendsdrinks-membership-writer.application.id"));
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
                new andrewgrant.friendsdrinks.AvroBuilder(schemaRegistryUrl));
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
