package andrewgrant.friendsdrinks.frontend.kafkastreams;

import static andrewgrant.friendsdrinks.TopicNameConfigKey.FRIENDSDRINKS_STATE;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;
import static andrewgrant.friendsdrinks.meetup.TopicNameConfigKey.FRIENDSDRINKS_MEETUP_STATE;
import static andrewgrant.friendsdrinks.membership.TopicNameConfigKey.*;
import static andrewgrant.friendsdrinks.user.TopicNameConfigKey.USER_STATE;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.frontend.RequestsPurger;
import andrewgrant.friendsdrinks.frontend.TopicNameConfigKey;
import andrewgrant.friendsdrinks.streamsconfig.Config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

/**
 * Responsible for building state stores needed by frontend.
 */
public class MaterializedViewsService {

    private static final Logger log = LoggerFactory.getLogger(MaterializedViewsService.class);

    public static final String RESPONSES_STATE_STORE = "api-response-state-store";
    public static final String FRIENDSDRINKS_STATE_STORE = "friendsdrinks-state-store";
    public static final String FRIENDSDRINKS_DETAIL_PAGE_STATE_STORE = "friendsdrinks-detail-page-state-store";
    public static final String USER_HOMEPAGES_STATE_STORE = "user-homepages-state-store";
    public static final String USERS_STATE_STORE = "users-state-store";
    public static final String INVITATIONS_STORE = "invitations-state-store";
    public static final String MEMBERSHIP_FRIENDSDRINKS_ID_STORE = "membership-friendsdrinks-id-state-store";

    private Properties envProps;
    private andrewgrant.friendsdrinks.AvroBuilder avroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder;
    private andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder;
    private andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder;
    private andrewgrant.friendsdrinks.meetup.AvroBuilder meetupAvroBuilder;

    public MaterializedViewsService(Properties envProps,
                                    andrewgrant.friendsdrinks.AvroBuilder avroBuilder,
                                    andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
                                    andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder,
                                    andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder,
                                    andrewgrant.friendsdrinks.meetup.AvroBuilder meetupAvroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
        this.apiAvroBuilder = apiAvroBuilder;
        this.membershipAvroBuilder = membershipAvroBuilder;
        this.userAvroBuilder = userAvroBuilder;
        this.meetupAvroBuilder = meetupAvroBuilder;
    }

    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String apiTopicName = envProps.getProperty(FRIENDSDRINKS_API);

        KStream<String, ApiEvent> apiEvents = builder.stream(apiTopicName,
                Consumed.with(Serdes.String(), apiAvroBuilder.apiEventSerde()));

        buildResponsesStore(builder, apiEvents);

        final String friendsDrinksStateTopicName = envProps.getProperty(FRIENDSDRINKS_STATE);
        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksKTable = builder.table(friendsDrinksStateTopicName,
                Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()),
                Materialized.<FriendsDrinksId, FriendsDrinksState, KeyValueStore<Bytes, byte[]>>
                        as(FRIENDSDRINKS_STATE_STORE)
                        .withKeySerde(avroBuilder.friendsDrinksIdSerde())
                        .withValueSerde(avroBuilder.friendsDrinksStateSerde()));


        KTable<String, UserState> userState = builder.stream(envProps.getProperty(USER_STATE),
                Consumed.with(userAvroBuilder.userIdSerde(), userAvroBuilder.userStateSerde()))
                .selectKey((k, v) -> k.getUserId())
                .toTable(
                        Materialized.<String, UserState, KeyValueStore<Bytes, byte[]>>
                                as(USERS_STATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userAvroBuilder.userStateSerde())
                );

        KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKTable =
                builder.table(envProps.getProperty(FRIENDSDRINKS_MEMBERSHIP_STATE),
                        Consumed.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                                membershipAvroBuilder.friendsDrinksMembershipStateSerdes()));

        membershipStateKTable.groupBy((key, value) -> KeyValue.pair(value.getMembershipId().getFriendsDrinksId(), value),
                Grouped.with(avroBuilder.friendsDrinksIdSerde(), membershipAvroBuilder.friendsDrinksMembershipStateSerdes())
        ).aggregate(
                () -> FriendsDrinksMembershipIdList.newBuilder().build(),
                (aggKey, newValue, aggValue) -> {
                    List<FriendsDrinksMembershipId> membershipIds = aggValue.getIds();
                    if (membershipIds == null) {
                        membershipIds = new ArrayList<>();
                    }
                    membershipIds.add(newValue.getMembershipId());
                    return FriendsDrinksMembershipIdList
                            .newBuilder(aggValue)
                            .setIds(membershipIds)
                            .build();
                },
                (aggKey, oldValue, aggValue) -> {
                    List<FriendsDrinksMembershipId> membershipIds = aggValue.getIds();
                    if (membershipIds == null) {
                        return aggValue;
                    }
                    for (int i = 0; i < membershipIds.size(); i++) {
                        FriendsDrinksMembershipId friendsDrinksMembershipId = membershipIds.get(i);
                        if (oldValue.getMembershipId().equals(friendsDrinksMembershipId)) {
                            membershipIds.remove(i);
                            break;
                        }
                    }
                    return aggValue;
                },
                Materialized.<FriendsDrinksId, FriendsDrinksMembershipIdList, KeyValueStore<Bytes, byte[]>>
                        as(MEMBERSHIP_FRIENDSDRINKS_ID_STORE)
                        .withKeySerde(avroBuilder.friendsDrinksIdSerde())
                        .withValueSerde(membershipAvroBuilder.friendsDrinksMembershipIdListSerdes())
        );

        KTable<FriendsDrinksMeetupId, FriendsDrinksMeetupState> friendsDrinksMeetupStateKTable =
                builder.table(envProps.getProperty(FRIENDSDRINKS_MEETUP_STATE),
                        Consumed.with(meetupAvroBuilder.friendsDrinksMeetupIdSpecificAvroSerde(),
                                meetupAvroBuilder.friendsDrinksMeetupStateSpecificAvroSerde()));

        buildFriendsDrinksDetailPageStateStore(membershipStateKTable, friendsDrinksKTable, userState, friendsDrinksMeetupStateKTable);

        final String invitationTopicName = envProps.getProperty(FRIENDSDRINKS_INVITATION_STATE);
        KTable<FriendsDrinksMembershipId, FriendsDrinksInvitationState> invitationStateKTable = builder.table(invitationTopicName,
                Consumed.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                        membershipAvroBuilder.friendsDrinksInvitationStateSerde()));

        invitationStateKTable.join(friendsDrinksKTable,
                (l -> l.getMembershipId().getFriendsDrinksId()),
                (l, r) -> InvitationStateFriendsDrinksEnriched
                        .newBuilder()
                        .setMembershipId(l.getMembershipId())
                        .setMessage(l.getMessage())
                        .setFriendsDrinksState(r)
                        .build(),
                Materialized.<FriendsDrinksMembershipId, InvitationStateFriendsDrinksEnriched, KeyValueStore<Bytes, byte[]>>
                        as(INVITATIONS_STORE)
                        .withKeySerde(membershipAvroBuilder.friendsDrinksMembershipIdSerdes())
                        .withValueSerde(apiAvroBuilder.invitationStateEnrichedSerde())
        );

        KStream<String, UserHomepage> userHomepageKStream =
                buildUserHomepageView(invitationStateKTable, membershipStateKTable, friendsDrinksKTable, userState);
        userHomepageKStream.toTable(
                Materialized.<String, UserHomepage, KeyValueStore<Bytes, byte[]>>
                        as(USER_HOMEPAGES_STATE_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(apiAvroBuilder.userHomepageSerde()));
        return builder.build();
    }

    private KStream<String, UserHomepage> buildUserHomepageView(
            KTable<FriendsDrinksMembershipId, FriendsDrinksInvitationState> invitationStateKTable,
            KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKTable,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
            KTable<String, UserState> userStateKTable) {

        KTable<String, InvitationStateFriendsDrinksEnrichedList> invitations =
                pendingInvitationsKeyedByUser(invitationStateKTable, friendsDrinksStateKTable);
        KTable<String, FriendsDrinksStateList> memberships = membershipsKeyedByUser(membershipStateKTable, friendsDrinksStateKTable);
        KTable<String, FriendsDrinksStateList> admins = friendsDrinksKeyedByAdmin(friendsDrinksStateKTable);

        return userStateKTable.mapValues(v -> {
            if (v == null || v.getStatus().equals(UserStatus.DELETED)) {
                return null;
            }
            UserHomepage userHomepage = UserHomepage
                    .newBuilder()
                    .setUserId(v.getUserId().getUserId())
                    .build();
            return userHomepage;
        }).leftJoin(invitations,
                (l, r) -> {
                    if (l == null) {
                        return null;
                    }
                    l.setInvitations(r);
                    return l;
                },
                Materialized.with(Serdes.String(), apiAvroBuilder.userHomepageSerde())
        ).leftJoin(memberships,
                (l, r) -> {
                    if (l == null) {
                        return null;
                    }
                    l.setMemberFriendsDrinks(r);
                    return l;
                },
                Materialized.with(Serdes.String(), apiAvroBuilder.userHomepageSerde())
        ).leftJoin(admins,
                (l, r) -> {
                    if (l == null) {
                        return null;
                    }
                    l.setAdminFriendsDrinks(r);
                    return l;
                },
                Materialized.with(Serdes.String(), apiAvroBuilder.userHomepageSerde())
        ).toStream();
    }

    private KTable<String, InvitationStateFriendsDrinksEnrichedList> pendingInvitationsKeyedByUser(
            KTable<FriendsDrinksMembershipId, FriendsDrinksInvitationState> invitationStateKTable,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        return invitationStateKTable.mapValues(v -> {
            if (v == null || !v.getStatus().equals(InvitationStatus.PENDING)) {
                return null;
            } else {
                return v;
            }
        }).join(friendsDrinksStateKTable,
                (l -> l.getMembershipId().getFriendsDrinksId()),
                (l, r) -> {
                    if (r.getStatus().equals(FriendsDrinksStatus.DELETED)) {
                        return null;
                    } else {
                        return InvitationStateFriendsDrinksEnriched
                                .newBuilder()
                                .setMembershipId(l.getMembershipId())
                                .setFriendsDrinksState(r)
                                .setMessage(l.getMessage())
                                .build();
                    }
                },
                Materialized.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(), apiAvroBuilder.invitationStateEnrichedSerde()))
                .groupBy((key, value) -> KeyValue.pair(value.getMembershipId().getUserId().getUserId(), value),
                        Grouped.with(Serdes.String(), apiAvroBuilder.invitationStateEnrichedSerde()))
                .aggregate(
                        () -> InvitationStateFriendsDrinksEnrichedList.newBuilder().setInvitations(new ArrayList<>()).build(),
                        (aggKey, newValue, aggValue) -> {
                            List<InvitationStateFriendsDrinksEnriched> invitations = aggValue.getInvitations();
                            invitations.add(newValue);
                            return InvitationStateFriendsDrinksEnrichedList
                                    .newBuilder()
                                    .setInvitations(invitations)
                                    .build();
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<InvitationStateFriendsDrinksEnriched> invitations = aggValue.getInvitations();
                            for (int i = 0; i < invitations.size(); i++) {
                                FriendsDrinksMembershipId friendsDrinksMembershipId = invitations.get(i).getMembershipId();
                                if (oldValue.getMembershipId().equals(friendsDrinksMembershipId)) {
                                    invitations.remove(i);
                                    break;
                                }
                            }
                            return InvitationStateFriendsDrinksEnrichedList
                                    .newBuilder()
                                    .setInvitations(invitations)
                                    .build();
                        },
                        Materialized.with(Serdes.String(), apiAvroBuilder.invitationStateEnrichedListSerde())
                );
    }

    private KTable<String, FriendsDrinksStateList> friendsDrinksKeyedByAdmin(
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        return friendsDrinksStateKTable.mapValues(v -> {
            if (v == null || v.getStatus().equals(FriendsDrinksStatus.DELETED)) {
                return null;
            } else {
                return v;
            }
        }).groupBy(((key, value) -> KeyValue.pair(value.getAdminUserId(), value)),
                Grouped.with(Serdes.String(), avroBuilder.friendsDrinksStateSerde()))
                .aggregate(
                        () -> FriendsDrinksStateList.newBuilder().setFriendsDrinks(new ArrayList<>()).build(),
                        (aggKey, newValue, aggValue) -> {
                            List<FriendsDrinksState> friendsDrinksStates = aggValue.getFriendsDrinks();
                            friendsDrinksStates.add(newValue);
                            return FriendsDrinksStateList
                                    .newBuilder()
                                    .setFriendsDrinks(friendsDrinksStates)
                                    .build();
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<FriendsDrinksState> friendsDrinksStates = aggValue.getFriendsDrinks();
                            for (int i = 0; i < friendsDrinksStates.size(); i++) {
                                FriendsDrinksId friendsDrinksId = friendsDrinksStates.get(i).getFriendsDrinksId();
                                if (friendsDrinksId.equals(oldValue.getFriendsDrinksId())) {
                                    friendsDrinksStates.remove(i);
                                    break;
                                }
                            }
                            return FriendsDrinksStateList
                                    .newBuilder()
                                    .setFriendsDrinks(friendsDrinksStates)
                                    .build();
                        },
                        Materialized.<String, FriendsDrinksStateList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-keyed-by-admin-state-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(apiAvroBuilder.friendsDrinksStateListSerde())
                );
    }

    private KTable<String, FriendsDrinksStateList> membershipsKeyedByUser(
            KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKTable,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        return membershipStateKTable.mapValues(v -> {
            if (v == null || v.getStatus().equals(FriendsDrinksMembershipStatus.REMOVED)) {
                return null;
            } else {
                return v;
            }
        }).join(friendsDrinksStateKTable,
                (l -> l.getMembershipId().getFriendsDrinksId()),
                (l, r) -> {
                    if (r.getStatus().equals(FriendsDrinksStatus.DELETED)) {
                        return null;
                    } else {
                        return MembershipStateFriendsDrinksEnriched
                                .newBuilder()
                                .setMembershipId(l.getMembershipId())
                                .setFriendsDrinksState(r)
                                .build();
                    }
                },
                Materialized.with(
                        membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                        apiAvroBuilder.membershipStateFriendsDrinksEnrichedSerde())
        ).groupBy((key, value) -> KeyValue.pair(value.getMembershipId().getUserId().getUserId(), value),
                Grouped.with(Serdes.String(), apiAvroBuilder.membershipStateFriendsDrinksEnrichedSerde()))
                .aggregate(
                        () -> FriendsDrinksStateList.newBuilder().setFriendsDrinks(new ArrayList<>()).build(),
                        (aggKey, newValue, aggValue) -> {
                            List<FriendsDrinksState> friendsDrinksStates = aggValue.getFriendsDrinks();
                            friendsDrinksStates.add(newValue.getFriendsDrinksState());
                            return FriendsDrinksStateList
                                    .newBuilder()
                                    .setFriendsDrinks(friendsDrinksStates)
                                    .build();
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<FriendsDrinksState> friendsDrinksStates = aggValue.getFriendsDrinks();
                            for (int i = 0; i < friendsDrinksStates.size(); i++) {
                                FriendsDrinksId friendsDrinksId = friendsDrinksStates.get(i).getFriendsDrinksId();
                                if (friendsDrinksId.equals(oldValue.getFriendsDrinksState().getFriendsDrinksId())) {
                                    friendsDrinksStates.remove(i);
                                    break;
                                }
                            }
                            return FriendsDrinksStateList
                                    .newBuilder()
                                    .setFriendsDrinks(friendsDrinksStates)
                                    .build();
                        },
                        Materialized.<String, FriendsDrinksStateList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-membership-keyed-by-user-state-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(apiAvroBuilder.friendsDrinksStateListSerde())
                );
    }

    private void buildResponsesStore(StreamsBuilder builder,
                                     KStream<String, ApiEvent> stream) {
        final String responsesTopicName = envProps.getProperty(TopicNameConfigKey.FRONTEND_RESPONSES_TOPIC_NAME);
        stream.filter(((key, value) -> {
            ApiEventType apiEventType = value.getEventType();
            if (apiEventType.equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT)) {
                return value.getFriendsDrinksMembershipEvent().getEventType()
                        .equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) ||
                        value.getFriendsDrinksMembershipEvent().getEventType()
                                .equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE);
            }
            FriendsDrinksApiEventType eventType = value.getFriendsDrinksEvent().getEventType();
            return eventType.equals(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_RESPONSE) ||
                    eventType.equals(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_RESPONSE) ||
                    eventType.equals(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_RESPONSE);
        })).to(responsesTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiEventSerde()));

        KStream<String, ApiEvent> responsesStream =
                builder.stream(responsesTopicName, Consumed.with(Serdes.String(), apiAvroBuilder.apiEventSerde()));

        // KTable for getting response results.
        responsesStream.toTable(Materialized.<String, ApiEvent, KeyValueStore<Bytes, byte[]>>
                as(RESPONSES_STATE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(apiAvroBuilder.apiEventSerde()));

        // Logic to tombstone responses in api-response-state-store so the KTable doesn't grow indefinitely.
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RequestsPurger.RESPONSES_PENDING_DELETION),
                Serdes.String(),
                apiAvroBuilder.apiEventSerde());
        builder.addStateStore(storeBuilder);
        responsesStream.transform(() -> new RequestsPurger(), RequestsPurger.RESPONSES_PENDING_DELETION)
                .filter((key, value) -> value != null && !value.isEmpty()).flatMapValues(value -> value)
                .selectKey((key, value) -> value).mapValues(value -> (ApiEvent) null)
                .to(responsesTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiEventSerde()));
    }

    private void buildFriendsDrinksDetailPageStateStore(
            KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKTableAll,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
            KTable<String, UserState> userStateKTable,
            KTable<FriendsDrinksMeetupId, FriendsDrinksMeetupState> friendsDrinksMeetupStateKTable) {

        KTable<FriendsDrinksMembershipId, MembershipStateUserEnriched> membershipStateKTable = membershipStateKTableAll
                .mapValues(value -> {
                    if (value.getStatus().equals(FriendsDrinksMembershipStatus.REMOVED)) {
                        return null;
                    } else {
                        MembershipStateUserEnriched enrichedMembershipState = MembershipStateUserEnriched.newBuilder()
                                .setMembershipId(value.getMembershipId())
                                .build();
                        return enrichedMembershipState;
                    }
                });

        KTable<FriendsDrinksMembershipId, MembershipStateUserEnriched> enrichedMembershipStateKTable =
                membershipStateKTable.leftJoin(userStateKTable,
                        (enrichedMembershipState -> enrichedMembershipState.getMembershipId().getUserId().getUserId()),
                        (l, r) -> MembershipStateUserEnriched
                                .newBuilder(l)
                                .setUserEmail(r.getEmail())
                                .setUserFirstName(r.getFirstName())
                                .setUserLastName(r.getLastName())
                                .build(),
                        Materialized.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                                apiAvroBuilder.friendsDrinksEnrichedMembershipStateSerde())
                );


        KTable<FriendsDrinksId, FriendsDrinksMeetupStateList> friendsDrinksMeetupStateListKTable =
                friendsDrinksMeetupStateKTable.groupBy((key, value) -> KeyValue.pair(value.getFriendsDrinksId(), value),
                        Grouped.with(avroBuilder.friendsDrinksIdSerde(), meetupAvroBuilder.friendsDrinksMeetupStateSpecificAvroSerde()))
                        .aggregate(
                                () -> FriendsDrinksMeetupStateList.newBuilder().setFriendsDrinksMeetupStates(new ArrayList<>()).build(),
                                (aggKey, newValue, aggValue) -> {
                                    List<FriendsDrinksMeetupState> states = aggValue.getFriendsDrinksMeetupStates();
                                    states.add(newValue);
                                    return FriendsDrinksMeetupStateList
                                            .newBuilder()
                                            .setFriendsDrinksMeetupStates(states)
                                            .build();
                                },
                                (aggKey, oldValue, aggValue) ->  {
                                    List<FriendsDrinksMeetupState> states = aggValue.getFriendsDrinksMeetupStates();
                                    for (int i = 0; i < states.size(); i++) {
                                        if (states.get(i).getMeetupId().getUuid().equals(
                                                oldValue.getMeetupId().getUuid())) {
                                            states.remove(i);
                                            break;
                                        }
                                    }
                                    return FriendsDrinksMeetupStateList
                                            .newBuilder()
                                            .setFriendsDrinksMeetupStates(states)
                                            .build();
                                },
                                Materialized.with(
                                        avroBuilder.friendsDrinksIdSerde(),
                                        meetupAvroBuilder.friendsDrinksMeetupStateListSpecificAvroSerde())
                        );

        KTable<FriendsDrinksId, UserStateList> enrichedMemberList = enrichedMembershipStateKTable.groupBy((key, value) ->
                        KeyValue.pair(value.getMembershipId().getFriendsDrinksId(), value),
                Grouped.with(avroBuilder.friendsDrinksIdSerde(), apiAvroBuilder.friendsDrinksEnrichedMembershipStateSerde()))
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
                        Materialized.with(avroBuilder.friendsDrinksIdSerde(), apiAvroBuilder.userStateListSerde())
                );

        friendsDrinksStateKTable.leftJoin(enrichedMemberList,
                (l, r) -> {
                    List<UserState> userStates = new ArrayList<>();
                    if (r != null && r.getUserStates() != null) {
                        userStates = r.getUserStates();
                    }
                    return FriendsDrinksDetailPage
                            .newBuilder()
                            .setName(l.getName())
                            .setFriendsDrinksId(l.getFriendsDrinksId())
                            .setMembers(userStates)
                            .setStatus(l.getStatus())
                            .setAdminUserId(l.getAdminUserId())
                            .build();
                },
                Materialized.with(avroBuilder.friendsDrinksIdSerde(), apiAvroBuilder.friendsDrinksDetailPageSerde())
        ).leftJoin(friendsDrinksMeetupStateListKTable,
                (l, r) -> {
                    List<FriendsDrinksMeetupState> meetupStates = new ArrayList<>();
                    if (r != null && (r.getFriendsDrinksMeetupStates() != null)) {
                        meetupStates = r.getFriendsDrinksMeetupStates();
                    }
                    return FriendsDrinksDetailPage
                            .newBuilder(l)
                            .setMeetups(meetupStates)
                            .build();
                },
                Materialized.with(avroBuilder.friendsDrinksIdSerde(), apiAvroBuilder.friendsDrinksDetailPageSerde())
        ).leftJoin(userStateKTable,
                (friendsDrinksAggregate -> friendsDrinksAggregate.getAdminUserId()),
                (l, r) -> {
                    List<UserState> members = l.getMembers();
                    if (r != null) {
                        UserState adminUserState = UserState
                                .newBuilder()
                                .setUserId(r.getUserId())
                                .setFirstName(r.getFirstName())
                                .setLastName(r.getLastName())
                                .setEmail(r.getEmail())
                                .setStatus(r.getStatus())
                                .build();
                        members.add(adminUserState);
                    }
                    return FriendsDrinksDetailPage
                            .newBuilder(l)
                            .setMembers(members)
                            .build();
                },
                Materialized.<FriendsDrinksId, FriendsDrinksDetailPage, KeyValueStore<Bytes, byte[]>>
                        as(FRIENDSDRINKS_DETAIL_PAGE_STATE_STORE)
                        .withKeySerde(avroBuilder.friendsDrinksIdSerde())
                        .withValueSerde(apiAvroBuilder.friendsDrinksDetailPageSerde())
        );
    }

    public Properties buildStreamsProperties(String portStr) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("frontend-api-application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        streamProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        if (envProps.getProperty("streams.dir") != null) {
            streamProps.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("streams.dir"));
        }
        if (envProps.getProperty("deployment-mode").equals("distributed")) {
            String podName = System.getenv("POD_NAME");
            String headlessServiceName = System.getenv("HEADLESS_SERVICE_NAME");
            String podNamespace = System.getenv("POD_NAMESPACE");
            if (podName != null && headlessServiceName != null && podNamespace != null) {
                String host = String.format("%s.%s.%s.svc.cluster.local",
                        System.getenv("POD_NAME"),
                        System.getenv("HEADLESS_SERVICE_NAME"),
                        System.getenv("POD_NAMESPACE"));
                String port = portStr;
                String endpoint = host + ":" + port;
                log.info("Setting {} to {}", StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
                streamProps.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
            } else {
                throw new RuntimeException(String.format("Deployment mode is distributed but can't find required metadata. " +
                        "Pod name: %s. Headless service name %s. Pod namespace %s.", podName, headlessServiceName, podNamespace));
            }
        }
        streamProps = Config.addSharedConfig(streamProps);
        return streamProps;
    }

}
