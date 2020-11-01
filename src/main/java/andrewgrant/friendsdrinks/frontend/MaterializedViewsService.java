package andrewgrant.friendsdrinks.frontend;

import static andrewgrant.friendsdrinks.TopicNameConfigKey.FRIENDSDRINKS_KEYED_BY_ADMIN_USER_ID_STATE;
import static andrewgrant.friendsdrinks.TopicNameConfigKey.FRIENDSDRINKS_STATE;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;
import static andrewgrant.friendsdrinks.membership.TopicNameConfigKey.*;
import static andrewgrant.friendsdrinks.user.TopicNameConfigKey.USER_STATE;

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

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipState;
import andrewgrant.friendsdrinks.user.AvroBuilder;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building state stores needed by frontend.
 */
public class MaterializedViewsService {

    public static final String RESPONSES_STORE = "api-response-state-store";
    public static final String FRIENDSDRINKS_STORE = "friendsdrinks-state-store";
    public static final String FRIENDSDRINKS_DETAIL_PAGE_STORE = "friendsdrinks-detail-page-state-store";
    public static final String MEMBERS_STORE = "members-state-store";
    public static final String ADMINS_STORE = "admins-state-store";
    public static final String USERS_STORE = "users-state-store";
    public static final String FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE = "friendsdrinks-keyed-by-single-id-state-store";
    public static final String INVITATIONS_STORE = "invitations-state-store";

    private Properties envProps;
    private andrewgrant.friendsdrinks.AvroBuilder avroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder;
    private andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder;
    private AvroBuilder userAvroBuilder;

    public MaterializedViewsService(Properties envProps,
                                    andrewgrant.friendsdrinks.AvroBuilder avroBuilder,
                                    andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
                                    andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder,
                                    AvroBuilder userAvroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
        this.apiAvroBuilder = apiAvroBuilder;
        this.membershipAvroBuilder = membershipAvroBuilder;
        this.userAvroBuilder = userAvroBuilder;
    }

    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String apiTopicName = envProps.getProperty(FRIENDSDRINKS_API);

        KStream<String, andrewgrant.friendsdrinks.api.avro.ApiEvent> apiEvents =
                builder.stream(apiTopicName,
                        Consumed.with(Serdes.String(), apiAvroBuilder.apiSerde()));

        final String frontendPrivateTopicName = envProps.getProperty(TopicNameConfigKey.FRONTEND_RESPONSES_TOPIC_NAME);
        buildResponsesStore(builder, apiEvents, frontendPrivateTopicName);

        builder.stream(envProps.getProperty(FRIENDSDRINKS_KEYED_BY_ADMIN_USER_ID_STATE),
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
                envProps.getProperty(FRIENDSDRINKS_MEMBERSHIP_KEYED_BY_USER_ID_STATE),
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


        final String friendsDrinksStateTopicName = envProps.getProperty(FRIENDSDRINKS_STATE);
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

        final String invitationTopicName = envProps.getProperty(FRIENDSDRINKS_INVITATION);
        builder.table(invitationTopicName,
                Consumed.with(membershipAvroBuilder.friendsDrinksInvitationIdSerde(),
                        membershipAvroBuilder.friendsDrinksInvitationSerde()),
                Materialized.as(INVITATIONS_STORE));

        KTable<String, andrewgrant.friendsdrinks.api.avro.UserState> userState =
                builder.stream(envProps.getProperty(USER_STATE),
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

        KStream<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKTable =
                builder.stream(envProps.getProperty(FRIENDSDRINKS_MEMBERSHIP_STATE),
                        Consumed.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                                membershipAvroBuilder.friendsDrinksMembershipStateSerdes()));

        buildFriendsDrinksDetailPageStateStore(membershipStateKTable,
                apiFriendsDrinksStateKTable, userState);
        return builder.build();
    }

    private void buildResponsesStore(StreamsBuilder builder,
                                     KStream<String, andrewgrant.friendsdrinks.api.avro.ApiEvent> stream,
                                     String responsesTopicName) {
        stream.filter(((key, value) -> {
            ApiEventType apiEventType = value.getEventType();
            if (!apiEventType.equals(ApiEventType.FRIENDSDRINKS_EVENT)) {
                return value.getEventType().equals(ApiEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) ||
                        value.getEventType().equals(ApiEventType.FRIENDSDRINKS_REMOVE_USER_RESPONSE) ||
                        value.getEventType().equals(ApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE);
            }
            FriendsDrinksEventType eventType = value.getFriendsDrinksEvent().getEventType();
            return eventType.equals(FriendsDrinksEventType.CREATE_FRIENDSDRINKS_RESPONSE) ||
                    eventType.equals(FriendsDrinksEventType.UPDATE_FRIENDSDRINKS_RESPONSE) ||
                    eventType.equals(FriendsDrinksEventType.DELETE_FRIENDSDRINKS_RESPONSE);
        })).to(responsesTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiSerde()));

        KStream<String, ApiEvent> responsesStream =
                builder.stream(responsesTopicName, Consumed.with(Serdes.String(), apiAvroBuilder.apiSerde()));

        // KTable for getting response results.
        responsesStream.toTable(Materialized.<String, ApiEvent, KeyValueStore<Bytes, byte[]>>
                as(RESPONSES_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(apiAvroBuilder.apiSerde()));

        // Logic to tombstone responses in responsesTopicName so the KTable doesn't grow indefinitely.
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RequestsPurger.RESPONSES_PENDING_DELETION),
                Serdes.String(),
                apiAvroBuilder.apiSerde());
        builder.addStateStore(storeBuilder);
        responsesStream.transform(() -> new RequestsPurger(), RequestsPurger.RESPONSES_PENDING_DELETION)
                .filter((key, value) -> value != null && !value.isEmpty()).flatMapValues(value -> value)
                .selectKey((key, value) -> value).mapValues(value -> (ApiEvent) null)
                .to(responsesTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiSerde()));
    }

    private void buildFriendsDrinksDetailPageStateStore(
            KStream<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKStream,
            KTable<String, FriendsDrinksState> friendsDrinksStateKTable,
            KTable<String, andrewgrant.friendsdrinks.api.avro.UserState> userStateKTable) {

        KTable<andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipId, FriendsDrinksEnrichedMembershipState>
                membershipStateKTable = membershipStateKStream
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
                .toTable(
                        Materialized.<FriendsDrinksMembershipId, FriendsDrinksEnrichedMembershipState, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-membership-enriched-bootstrap-state-store")
                                .withKeySerde(apiAvroBuilder.apiFriendsDrinksMembershipIdSerde())
                                .withValueSerde(apiAvroBuilder.apiFriendsDrinksEnrichedMembershipStateSerde()));

        KTable<FriendsDrinksMembershipId, FriendsDrinksEnrichedMembershipState> enrichedMembershipStateKTable =
                membershipStateKTable.leftJoin(userStateKTable,
                        (enrichedMembershipState -> enrichedMembershipState.getMembershipId().getUserId().getUserId()),
                        (l, r) -> FriendsDrinksEnrichedMembershipState
                                .newBuilder(l)
                                .setUserEmail(r.getEmail())
                                .setUserFirstName(r.getFirstName())
                                .setUserLastName(r.getLastName())
                                .build(),
                        Materialized.with(apiAvroBuilder.apiFriendsDrinksMembershipIdSerde(),
                                apiAvroBuilder.apiFriendsDrinksEnrichedMembershipStateSerde())
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
                                    .setUserId(newValue.getMembershipId().getUserId())
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
                        as("friendsdrinks-detail-page-state-store-tmp-1")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(apiAvroBuilder.apiFriendsDrinksAggregateSerdes())
        ).leftJoin(userStateKTable,
                (friendsDrinksAggregate -> friendsDrinksAggregate.getFriendsDrinksId().getAdminUserId()),
                (l, r) -> {
                    List<UserState> members = l.getMembers();
                    if (r != null) {
                        UserState adminUserState = UserState
                                .newBuilder()
                                .setUserId(r.getUserId())
                                .setFirstName(r.getFirstName())
                                .setLastName(r.getLastName())
                                .setEmail(r.getEmail())
                                .build();
                        members.add(adminUserState);
                    }
                    return FriendsDrinksAggregate
                            .newBuilder(l)
                            .setMembers(members)
                            .build();
                },
                Materialized.<String, FriendsDrinksAggregate, KeyValueStore<Bytes, byte[]>>
                        as(FRIENDSDRINKS_DETAIL_PAGE_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(apiAvroBuilder.apiFriendsDrinksAggregateSerdes())
        );
    }

    public Properties buildStreamsProperties(String uri) {
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

}
