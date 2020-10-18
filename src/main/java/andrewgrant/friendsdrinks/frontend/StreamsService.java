package andrewgrant.friendsdrinks.frontend;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import andrewgrant.friendsdrinks.AvroBuilder;
import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.user.UserAvroBuilder;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building state stores needed by frontend.
 */
public class StreamsService {

    public static final String RESPONSES_STORE = "api-response-state-store";
    public static final String FRIENDSDRINKS_STORE = "friendsdrinks-state-store";
    public static final String FRIENDSDRINKS_DETAIL_PAGE_STORE = "friendsdrinks-detail-page-state-store";
    public static final String MEMBERS_STORE = "members-state-store";
    public static final String ADMINS_STORE = "admins-state-store";
    public static final String USERS_STORE = "users-state-store";
    public static final String FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE = "friendsdrinks-keyed-by-single-id-state-store";
    public static final String PENDING_INVITATIONS_STORE = "pending-invitations-state-store";
    private KafkaStreams streams;

    public StreamsService(Properties envProps,
                          String uri,
                          AvroBuilder avroBuilder,
                          andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
                          andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder,
                          UserAvroBuilder userAvroBuilder) {
        Topology topology = buildTopology(envProps, avroBuilder, apiAvroBuilder, membershipAvroBuilder, userAvroBuilder);
        Properties streamProps = buildStreamsProperties(envProps, uri);
        streams = new KafkaStreams(topology, streamProps);
    }

    private Topology buildTopology(Properties envProps,
                                   AvroBuilder avroBuilder,
                                   andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
                                   andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder,
                                   UserAvroBuilder userAvroBuilder) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String apiTopicName = envProps.getProperty("friendsdrinks-api.topic.name");

        KStream<String, andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> apiEvents =
                builder.stream(apiTopicName,
                        Consumed.with(Serdes.String(), apiAvroBuilder.friendsDrinksSerde()));

        final String frontendPrivateTopicName = envProps.getProperty("frontend-responses.topic.name");
        buildResponsesStore(builder, apiEvents, apiAvroBuilder, frontendPrivateTopicName);

        builder.stream(envProps.getProperty("friendsdrinks-keyed-by-admin-user-id-state.topic.name"),
                Consumed.with(Serdes.String(), avroBuilder.friendsDrinksIdListSerde()))
                .mapValues(value -> {
                    if (value == null || value.getIds() == null) {
                        return null;
                    }
                    FriendsDrinksIdList idList = FriendsDrinksIdList
                            .newBuilder()
                            .setIds(value.getIds().stream().map(x -> andrewgrant.friendsdrinks.api.avro.FriendsDrinksId
                                    .newBuilder()
                                    .setUuid(x.getUuid())
                                    .setAdminUserId(x.getAdminUserId())
                                    .build()).collect(Collectors.toList()))
                            .build();
                    return idList;
                })
                .toTable(
                        Materialized.<String, FriendsDrinksIdList, KeyValueStore<Bytes, byte[]>>
                                as(ADMINS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(apiAvroBuilder.apiFriendsDrinksIdListSerde()));


        KStream<String, FriendsDrinksIdList> membersStream = builder.stream(
                envProps.getProperty("friendsdrinks-membership-keyed-by-user-id-state.topic.name"),
                Consumed.with(membershipAvroBuilder.userIdSerdes(), membershipAvroBuilder.friendsDrinksMembershipIdListSerdes()))
                .map((key, value) -> {
                    if (value == null) {
                        return KeyValue.pair(key.getUserId(), null);
                    }
                    String userId = key.getUserId();
                    FriendsDrinksIdList friendsDrinksIdList = FriendsDrinksIdList
                            .newBuilder()
                            .setIds(value.getIds().stream().map(x ->
                                    andrewgrant.friendsdrinks.api.avro.FriendsDrinksId.newBuilder()
                                            .setAdminUserId(x.getFriendsDrinksId().getAdminUserId())
                                            .setUuid(x.getFriendsDrinksId().getUuid())
                                            .build()
                            ).collect(Collectors.toList()))
                            .build();
                    return KeyValue.pair(userId, friendsDrinksIdList);
                });
        membersStream.toTable(
                Materialized.<String, FriendsDrinksIdList, KeyValueStore<Bytes, byte[]>>
                        as(MEMBERS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(apiAvroBuilder.apiFriendsDrinksIdListSerde()));


        final String friendsDrinksStateTopicName = envProps.getProperty("friendsdrinks-state.topic.name");
        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState>
                friendsDrinksKStream = builder.stream(friendsDrinksStateTopicName,
                Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        KStream<FriendsDrinksId, FriendsDrinksState> apiFriendsDrinksState = friendsDrinksKStream
                .map((key, value) -> {
                    FriendsDrinksId friendsDrinksIdApi = FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(key.getAdminUserId())
                            .setUuid(key.getUuid())
                            .build();
                    if (value == null) {
                        return KeyValue.pair(friendsDrinksIdApi, null);
                    }
                    FriendsDrinksState friendsDrinksStateApi = FriendsDrinksState
                            .newBuilder()
                            .setName(value.getName())
                            .setFriendsDrinksId(friendsDrinksIdApi)
                            .build();
                    return KeyValue.pair(friendsDrinksIdApi, friendsDrinksStateApi);
                });

        apiFriendsDrinksState.toTable(
                Materialized.<FriendsDrinksId, FriendsDrinksState, KeyValueStore<Bytes, byte[]>>
                        as(FRIENDSDRINKS_STORE)
                        .withKeySerde(apiAvroBuilder.apiFriendsDrinksIdSerde())
                        .withValueSerde(apiAvroBuilder.apiFriendsDrinksStateSerde()));

        KTable<String, FriendsDrinksState> apiFriendsDrinksStateKTable =
                apiFriendsDrinksState.map((key, value) -> KeyValue.pair(key.getUuid(), value))
                        .toTable(
                                Materialized.<String, FriendsDrinksState, KeyValueStore<Bytes, byte[]>>
                                        as(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE)
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(apiAvroBuilder.apiFriendsDrinksStateSerde()));

        final String pendingInvitationsTopicName = envProps.getProperty("friendsdrinks-pending-invitation.topic.name");
        builder.table(pendingInvitationsTopicName,
                Consumed.with(apiAvroBuilder.friendsDrinksPendingInvitationIdSerde(),
                        apiAvroBuilder.friendsDrinksPendingInvitationSerde()),
                Materialized.as(PENDING_INVITATIONS_STORE));

        KTable<String, andrewgrant.friendsdrinks.api.avro.UserState> userState =
                builder.stream(envProps.getProperty("user-state.topic.name"),
                        Consumed.with(userAvroBuilder.userIdSerde(), userAvroBuilder.userStateSerde()))
                        .map(((key, value) -> {
                            String newKey = key.getUserId();
                            if (value == null) {
                                return KeyValue.pair(newKey, null);
                            }
                            andrewgrant.friendsdrinks.api.avro.UserState apiUserState =
                                    andrewgrant.friendsdrinks.api.avro.UserState.newBuilder()
                                            .setUserId(UserId.newBuilder().setUserId(value.getUserId().getUserId()).build())
                                            .setEmail(value.getEmail())
                                            .setFirstName(value.getFirstName())
                                            .setLastName(value.getLastName())
                                            .build();
                            return KeyValue.pair(newKey, apiUserState);
                        }))
                        .toTable(
                                Materialized.<String, UserState, KeyValueStore<Bytes, byte[]>>
                                        as(USERS_STORE)
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(apiAvroBuilder.apiUserStateSerde())
                        );

        buildFriendsDrinksDetailPageStateStore(builder, envProps, membershipAvroBuilder, apiAvroBuilder,
                apiFriendsDrinksStateKTable, userState);
        return builder.build();
    }

    private void buildResponsesStore(StreamsBuilder builder,
                                     KStream<String, andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> stream,
                                     andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
                                     String responsesTopicName) {
        stream.filter(((key, value) -> {
            EventType eventType = value.getEventType();
            return eventType.equals(EventType.CREATE_FRIENDSDRINKS_RESPONSE) ||
                    eventType.equals(EventType.UPDATE_FRIENDSDRINKS_RESPONSE) ||
                    eventType.equals(EventType.FRIENDSDRINKS_INVITATION_RESPONSE) ||
                    eventType.equals(EventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) ||
                    eventType.equals(EventType.DELETE_FRIENDSDRINKS_RESPONSE);
        })).to(responsesTopicName, Produced.with(Serdes.String(), apiAvroBuilder.friendsDrinksSerde()));

        KStream<String, FriendsDrinksEvent> responsesStream =
                builder.stream(responsesTopicName, Consumed.with(Serdes.String(), apiAvroBuilder.friendsDrinksSerde()));

        // KTable for getting response results.
        responsesStream.toTable(Materialized.<String, FriendsDrinksEvent, KeyValueStore<Bytes, byte[]>>
                as(RESPONSES_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(apiAvroBuilder.friendsDrinksSerde()));

        // Logic to tombstone responses in responsesTopicName so the KTable doesn't grow indefinitely.
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RequestsPurger.RESPONSES_PENDING_DELETION),
                Serdes.String(),
                apiAvroBuilder.friendsDrinksSerde());
        builder.addStateStore(storeBuilder);
        responsesStream.transform(() -> new RequestsPurger(), RequestsPurger.RESPONSES_PENDING_DELETION)
                .filter((key, value) -> value != null && !value.isEmpty()).flatMapValues(value -> value)
                .selectKey((key, value) -> value).mapValues(value -> (FriendsDrinksEvent) null)
                .to(responsesTopicName, Produced.with(Serdes.String(), apiAvroBuilder.friendsDrinksSerde()));
    }

    private void buildFriendsDrinksDetailPageStateStore(
            StreamsBuilder builder, Properties envProps,
            andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder,
            andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
            KTable<String, FriendsDrinksState> friendsDrinksStateKTable,
            KTable<String, andrewgrant.friendsdrinks.api.avro.UserState> userStateKTable) {

        KTable<andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipId, FriendsDrinksEnrichedMembershipState> membershipStateKTable =
                builder.stream(envProps.getProperty("friendsdrinks-membership-state.topic.name"),
                        Consumed.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                                membershipAvroBuilder.friendsDrinksMembershipStateSerdes()))
                        .map((key, value) -> {
                            andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipId apiId =
                                    andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipId.newBuilder()
                                            .setFriendsDrinksId(FriendsDrinksId
                                                    .newBuilder()
                                                    .setUuid(key.getFriendsDrinksId().getUuid())
                                                    .setAdminUserId(key.getFriendsDrinksId().getAdminUserId())
                                                    .build())
                                            .setUserId(UserId.newBuilder()
                                                    .setUserId(key.getUserId().getUserId())
                                                    .build())
                                            .build();
                            if (value == null) {
                                return KeyValue.pair(apiId, null);
                            } else {
                                FriendsDrinksEnrichedMembershipState enrichedMembershipState =
                                        FriendsDrinksEnrichedMembershipState.newBuilder()
                                                .setMembershipId(apiId)
                                                .build();
                                return KeyValue.pair(apiId, enrichedMembershipState);
                            }
                        })
                        .toTable();

        KTable<FriendsDrinksMembershipId, FriendsDrinksEnrichedMembershipState> enrichedMembershipStateKTable =
                membershipStateKTable.join(userStateKTable,
                        (enrichedMembershipState -> enrichedMembershipState.getMembershipId().getUserId().getUserId()),
                        (l, r) -> FriendsDrinksEnrichedMembershipState
                                .newBuilder(l)
                                .setUserEmail(r.getEmail())
                                .setUserFirstName(r.getFirstName())
                                .setUserLastName(r.getLastName())
                                .build()
                );

        KTable<String, UserStateList> enrichedMemberList = enrichedMembershipStateKTable.groupBy((key, value) ->
                        KeyValue.pair(value.getMembershipId().getFriendsDrinksId().getUuid(), value),
                Grouped.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksEnrichedMembershipStateSerde()))
                .aggregate(
                        () -> UserStateList.newBuilder().setUserStates(new ArrayList<>()).build(),
                        (aggKey, newValue, aggValue) -> {
                            List<UserState> userStates = aggValue.getUserStates();
                            userStates.add(UserState
                                    .newBuilder()
                                    .setEmail(newValue.getUserEmail())
                                    .setFirstName(newValue.getUserFirstName())
                                    .setLastName(newValue.getUserLastName())
                                    .build());
                            return UserStateList
                                    .newBuilder()
                                    .setUserStates(userStates)
                                    .build();
                        },
                        (aggKey, oldValue, aggValue) ->  {
                            List<UserState> userStates = aggValue.getUserStates();
                            for (int i = 0; i < userStates.size(); i++) {
                                if (userStates.get(i).getUserId().getUserId().equals(
                                        oldValue.getMembershipId().getUserId().getUserId())) {
                                    userStates.remove(i);
                                    break;
                                }
                            }
                            return UserStateList
                                    .newBuilder()
                                    .setUserStates(userStates)
                                    .build();
                        },
                        Materialized.<String, UserStateList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-enriched-membership-list")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(apiAvroBuilder.apiUserStateListSerde())
                );

        friendsDrinksStateKTable.leftJoin(enrichedMemberList,
                (l, r) -> {
                    List<UserState> userStates = new ArrayList<>();
                    if (r != null && r.getUserStates() != null) {
                        userStates = r.getUserStates();
                    }
                    return FriendsDrinksAggregate
                            .newBuilder()
                            .setName(l.getName())
                            .setFriendsDrinksId(l.getFriendsDrinksId())
                            .setMembers(userStates)
                            .build();
                },
                Materialized.<String, FriendsDrinksAggregate, KeyValueStore<Bytes, byte[]>>
                        as(FRIENDSDRINKS_DETAIL_PAGE_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(apiAvroBuilder.apiFriendsDrinksAggregateSerdes())
        );
    }

    private static Properties buildStreamsProperties(Properties envProps, String uri) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("frontend-api-application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, uri);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return props;
    }

    public KafkaStreams getStreams() {
        return streams;
    }

}
