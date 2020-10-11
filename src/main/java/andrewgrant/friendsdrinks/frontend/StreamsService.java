package andrewgrant.friendsdrinks.frontend;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.stream.Collectors;

import andrewgrant.friendsdrinks.AvroBuilder;
import andrewgrant.friendsdrinks.api.avro.EventType;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksIdList;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.user.UserAvroBuilder;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    public static final String RESPONSES_STORE = "api-response-store";
    public static final String FRIENDSDRINKS_STORE = "friendsdrinks-store";
    public static final String MEMBERS_STORE = "members-store";
    public static final String ADMINS_STORE = "admins-store";
    public static final String USERS_STORE = "users-store";
    public static final String FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE = "friendsdrinks-keyed-by-single-id-store";
    public static final String PENDING_INVITATIONS_STORE = "pending-invitations-store";
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

        final String frontendPrivateTopicName = envProps.getProperty("frontend-private.topic.name");
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
                    if (value == null || value.getIds() == null) {
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
        KStream<FriendsDrinksId, FriendsDrinksState> friendsDrinksState =
                builder.stream(friendsDrinksStateTopicName,
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));
        friendsDrinksState.toTable(
                Materialized.<FriendsDrinksId, FriendsDrinksState, KeyValueStore<Bytes, byte[]>>
                        as(FRIENDSDRINKS_STORE)
                        .withKeySerde(avroBuilder.friendsDrinksIdSerde())
                        .withValueSerde(avroBuilder.friendsDrinksStateSerde()));

        friendsDrinksState.map((key, value) -> KeyValue.pair(key.getUuid(), value))
                .toTable(
                        Materialized.<String, FriendsDrinksState, KeyValueStore<Bytes, byte[]>>
                                as(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(avroBuilder.friendsDrinksStateSerde()));


        final String pendingInvitationsTopicName = envProps.getProperty("friendsdrinks-pending-invitation.topic.name");
        builder.table(pendingInvitationsTopicName,
                Consumed.with(apiAvroBuilder.friendsDrinksPendingInvitationIdSerde(), apiAvroBuilder.friendsDrinksPendingInvitationSerde()),
                Materialized.as(PENDING_INVITATIONS_STORE));

        builder.table(envProps.getProperty("user-state.topic.name"),
                Consumed.with(userAvroBuilder.userIdSerde(), userAvroBuilder.userStateSerde()),
                Materialized.as(USERS_STORE));

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

    private static Properties buildStreamsProperties(Properties envProps, String uri) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("frontend-application.id"));
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
