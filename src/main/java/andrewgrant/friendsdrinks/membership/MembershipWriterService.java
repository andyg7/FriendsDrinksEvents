package andrewgrant.friendsdrinks.membership;

import static andrewgrant.friendsdrinks.TopicNameConfigKey.FRIENDSDRINKS_EVENT;
import static andrewgrant.friendsdrinks.env.Properties.load;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;
import static andrewgrant.friendsdrinks.user.TopicNameConfigKey.USER_EVENT;

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
import java.util.stream.Collectors;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.api.avro.ApiEventType;
import andrewgrant.friendsdrinks.membership.avro.*;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipEvent;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId;
import andrewgrant.friendsdrinks.membership.avro.UserId;
import andrewgrant.friendsdrinks.user.AvroBuilder;
import andrewgrant.friendsdrinks.user.avro.UserEvent;

/**
 * Owns writing to friendsdrinks-membership-event.
 */
public class MembershipWriterService {

    private static final Logger log = LoggerFactory.getLogger(MembershipWriterService.class);

    private Properties envProps;
    private andrewgrant.friendsdrinks.membership.AvroBuilder avroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder;
    private andrewgrant.friendsdrinks.AvroBuilder friendsDrinksAvroBuilder;
    private AvroBuilder userAvroBuilder;

    public MembershipWriterService(Properties envProps, andrewgrant.friendsdrinks.membership.AvroBuilder avroBuilder,
                                   andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder,
                                   andrewgrant.friendsdrinks.AvroBuilder friendsDrinksAvroBuilder,
                                   AvroBuilder userAvroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
        this.frontendAvroBuilder = frontendAvroBuilder;
        this.friendsDrinksAvroBuilder = friendsDrinksAvroBuilder;
        this.userAvroBuilder = userAvroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ApiEvent> apiEvents = builder.stream(envProps.getProperty(FRIENDSDRINKS_API),
                Consumed.with(Serdes.String(), frontendAvroBuilder.apiSerde()));
        KStream<String, ApiEvent> successfulApiResponses = streamOfSuccessfulInvitationReplies(apiEvents);
        KStream<String, ApiEvent> apiRequests = streamOfAcceptedInvitations(apiEvents);

        successfulApiResponses.join(apiRequests,
                (l, r) -> new MembershipRequestResponseJoiner().join(r),
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        frontendAvroBuilder.apiSerde(),
                        frontendAvroBuilder.apiSerde()))
                .selectKey((k, v) -> v.getMembershipId())
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));

        KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> friendsDrinksMembershipEventKStream
                = builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));
        buildMembershipStateKTable(friendsDrinksMembershipEventKStream)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_STATE),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()));

        KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKTable =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_STATE),
                        Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()));

        buildMembershipIdListKeyedByFriendsDrinksIdView(membershipStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_KEYED_BY_FRIENDSDRINKS_ID_STATE),
                        Produced.with(avroBuilder.friendsDrinksIdSerdes(), avroBuilder.friendsDrinksMembershipIdListSerdes()));

        buildMembershipIdListKeyedByUserIdView(membershipStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_KEYED_BY_USER_ID_STATE),
                        Produced.with(avroBuilder.userIdSerdes(), avroBuilder.friendsDrinksMembershipIdListSerdes()));

        KTable<FriendsDrinksId, FriendsDrinksMembershipIdList> membershipIdListKeyedByFriendsDrinksIdStateKTable =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_KEYED_BY_FRIENDSDRINKS_ID_STATE),
                        Consumed.with(avroBuilder.friendsDrinksIdSerdes(), avroBuilder.friendsDrinksMembershipIdListSerdes()));

        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent>
                friendsDrinksEventKStream = builder.stream(envProps.getProperty(FRIENDSDRINKS_EVENT),
                Consumed.with(friendsDrinksAvroBuilder.friendsDrinksIdSerde(), friendsDrinksAvroBuilder.friendsDrinksEventSerde()));

        handleDeletedFriendsDrinks(friendsDrinksEventKStream, membershipIdListKeyedByFriendsDrinksIdStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));

        KTable<UserId, FriendsDrinksMembershipIdList> membershipIdListKeyedByUserIdStateKTable =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_KEYED_BY_USER_ID_STATE),
                        Consumed.with(avroBuilder.userIdSerdes(), avroBuilder.friendsDrinksMembershipIdListSerdes()));

        KStream<andrewgrant.friendsdrinks.user.avro.UserId, UserEvent> userEventKStream =
                builder.stream(envProps.getProperty(USER_EVENT),
                        Consumed.with(userAvroBuilder.userIdSerde(), userAvroBuilder.userEventSerde()));
        handleDeletedUsers(userEventKStream, membershipIdListKeyedByUserIdStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));

        return builder.build();
    }

    private KStream<FriendsDrinksId, FriendsDrinksMembershipIdList> buildMembershipIdListKeyedByFriendsDrinksIdView(
            KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> friendsDrinksMembershipStateKTable) {

        return friendsDrinksMembershipStateKTable.groupBy((key, value) ->
                        KeyValue.pair(value.getMembershipId().getFriendsDrinksId(), value),
                Grouped.with(avroBuilder.friendsDrinksIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()))
                .aggregate(
                        () -> FriendsDrinksMembershipIdList
                                .newBuilder()
                                .setIds(new ArrayList<>())
                                .build(),
                        (aggKey, newValue, aggValue) -> {
                            List<FriendsDrinksMembershipId> ids = aggValue.getIds();
                            ids.add(newValue.getMembershipId());
                            FriendsDrinksMembershipIdList idList = FriendsDrinksMembershipIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<FriendsDrinksMembershipId> ids = aggValue.getIds();
                            for (int i = 0; i < ids.size(); i++) {
                                if (ids.get(0).equals(oldValue.getMembershipId())) {
                                    ids.remove(i);
                                    break;
                                }
                            }
                            FriendsDrinksMembershipIdList idList = FriendsDrinksMembershipIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        Materialized.<FriendsDrinksId, FriendsDrinksMembershipIdList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-membership-id-list-keyed-by-friendsdrinks-id-state-store")
                                .withKeySerde(avroBuilder.friendsDrinksIdSerdes())
                                .withValueSerde(avroBuilder.friendsDrinksMembershipIdListSerdes())
                )
                .toStream();
    }

    private KStream<UserId, FriendsDrinksMembershipIdList> buildMembershipIdListKeyedByUserIdView(
            KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> friendsDrinksMembershipStateKTable) {

        return friendsDrinksMembershipStateKTable.groupBy((key, value) ->
                        KeyValue.pair(value.getMembershipId().getUserId(), value),
                Grouped.with(avroBuilder.userIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()))
                .aggregate(
                        () -> FriendsDrinksMembershipIdList
                                .newBuilder()
                                .setIds(new ArrayList<>())
                                .build(),
                        (aggKey, newValue, aggValue) -> {
                            List<FriendsDrinksMembershipId> ids = aggValue.getIds();
                            ids.add(newValue.getMembershipId());
                            FriendsDrinksMembershipIdList idList = FriendsDrinksMembershipIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<FriendsDrinksMembershipId> ids = aggValue.getIds();
                            for (int i = 0; i < ids.size(); i++) {
                                if (ids.get(0).equals(oldValue.getMembershipId())) {
                                    ids.remove(i);
                                    break;
                                }
                            }
                            if (ids.size() == 0) {
                                return null;
                            }
                            FriendsDrinksMembershipIdList idList = FriendsDrinksMembershipIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        Materialized.<UserId, FriendsDrinksMembershipIdList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-membership-id-list-keyed-by-user-id-state-store")
                                .withKeySerde(avroBuilder.userIdSerdes())
                                .withValueSerde(avroBuilder.friendsDrinksMembershipIdListSerdes())
                )
                .toStream();
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> handleDeletedUsers(
            KStream<andrewgrant.friendsdrinks.user.avro.UserId, UserEvent> userEventKStream,
            KTable<UserId, FriendsDrinksMembershipIdList> membershipIdListKTable) {

        KStream<UserId, FriendsDrinksMembershipEvent> streamOfMembershipIdsToDelete = userEventKStream
                .map((key, value) -> {
                    UserId userId = UserId
                            .newBuilder()
                            .setUserId(key.getUserId())
                            .build();
                    if (value.getEventType().equals(andrewgrant.friendsdrinks.user.avro.EventType.DELETED)) {
                        return KeyValue.pair(userId, value.getRequestId());
                    } else {
                        return KeyValue.pair(userId, null);
                    }
                })
                .filter((key, value) -> value != null)
                .leftJoin(membershipIdListKTable,
                        (l, r) -> FriendsDrinksMembershipEventList
                                .newBuilder()
                                .setEvents(r.getIds().stream().map(x -> FriendsDrinksMembershipEvent
                                        .newBuilder()
                                        .setRequestId(l)
                                        .setEventType(EventType.MEMBERSHIP_REMOVED)
                                        .setMembershipId(x)
                                        .setFriendsDrinksMembershipRemoved(FriendsDrinksMembershipRemoved
                                                .newBuilder()
                                                .setMembershipId(x)
                                                .build())
                                        .build()).collect(Collectors.toList()))
                                .build(),
                        Joined.with(
                                avroBuilder.userIdSerdes(),
                                Serdes.String(),
                                avroBuilder.friendsDrinksMembershipIdListSerdes()))
                .filter((key, value) -> value != null)
                .flatMapValues(value -> value.getEvents());

        return streamOfMembershipIdsToDelete.map(((key, value) ->
                KeyValue.pair(value.getMembershipId(), value)));
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> handleDeletedFriendsDrinks(
            KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent>
                    friendsDrinksEventKStream,
            KTable<FriendsDrinksId, FriendsDrinksMembershipIdList> membershipIdListKTable) {

        KStream<FriendsDrinksId, FriendsDrinksMembershipEvent> streamOfMembershipIdsToDelete = friendsDrinksEventKStream
                .map((key, value) -> {
                    FriendsDrinksId friendsDrinksId = FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(key.getAdminUserId())
                            .setUuid(key.getUuid())
                            .build();
                    if (value.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.DELETED)) {
                        return KeyValue.pair(friendsDrinksId, value.getRequestId());
                    } else {
                        return KeyValue.pair(friendsDrinksId, null);
                    }
                })
                .filter((key, value) -> value != null)
                .leftJoin(membershipIdListKTable,
                        (l, r) -> FriendsDrinksMembershipEventList
                                .newBuilder()
                                .setEvents(r.getIds().stream().map(x -> FriendsDrinksMembershipEvent
                                        .newBuilder()
                                        .setRequestId(l)
                                        .setEventType(EventType.MEMBERSHIP_REMOVED)
                                        .setMembershipId(x)
                                        .setFriendsDrinksMembershipRemoved(FriendsDrinksMembershipRemoved
                                                .newBuilder()
                                                .setMembershipId(x)
                                                .build())
                                        .build()).collect(Collectors.toList()))
                                .build(),
                        Joined.with(
                                avroBuilder.friendsDrinksIdSerdes(),
                                Serdes.String(),
                                avroBuilder.friendsDrinksMembershipIdListSerdes())
                )
                .filter((key, value) -> value != null)
                .flatMapValues(value -> value.getEvents());

        return streamOfMembershipIdsToDelete.map((key, value) -> KeyValue.pair(value.getMembershipId(), value));
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipState> buildMembershipStateKTable(
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> membershipEventKStream) {
        return membershipEventKStream.mapValues(value -> {
            if (value.getEventType().equals(andrewgrant.friendsdrinks.membership.avro.EventType.MEMBERSHIP_REMOVED)) {
                return null;
            } else {
                return FriendsDrinksMembershipState.newBuilder()
                        .setMembershipId(value.getMembershipId())
                        .setLastRequestId(value.getRequestId())
                        .build();
            }
        });
    }

    private KStream<String, ApiEvent> streamOfSuccessfulInvitationReplies(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) -> friendsDrinksEvent.getEventType()
                .equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT) &&
                (friendsDrinksEvent.getFriendsDrinksMembershipEvent().getEventType()
                        .equals(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) &&
                        friendsDrinksEvent.getFriendsDrinksMembershipEvent()
                                .getFriendsDrinksInvitationReplyResponse().getResult().equals(Result.SUCCESS)));
    }

    private KStream<String, ApiEvent> streamOfAcceptedInvitations(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((k, v) -> v.getEventType().equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT) &&
                (v.getFriendsDrinksMembershipEvent().getEventType().equals(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                && v.getFriendsDrinksMembershipEvent().getFriendsDrinksInvitationReplyRequest().getReply().equals(Reply.ACCEPTED));
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
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        MembershipWriterService writerService = new MembershipWriterService(
                envProps,
                new andrewgrant.friendsdrinks.membership.AvroBuilder(schemaRegistryUrl),
                new andrewgrant.friendsdrinks.frontend.AvroBuilder(schemaRegistryUrl),
                new andrewgrant.friendsdrinks.AvroBuilder(schemaRegistryUrl),
                new AvroBuilder(schemaRegistryUrl));
        Topology topology = writerService.buildTopology();
        Properties streamProps = writerService.buildStreamsProperties(envProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);
        log.info("Starting MembershipWriterService application...");

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
