package andrewgrant.friendsdrinks.frontend.restapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

import andrewgrant.friendsdrinks.AvroBuilder;
import andrewgrant.friendsdrinks.api.avro.EventType;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    public static final String RESPONSES_STORE = "api-response-store";
    public static final String FRIENDSDRINKS_STORE = "friendsdrinks-store";
    public static final String FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE = "friendsdrinks-keyed-by-single-id-store";
    public static final String PENDING_INVITATIONS_STORE = "pending-invitations-store";
    private KafkaStreams streams;

    public StreamsService(Properties envProps,
                          String uri,
                          AvroBuilder avroBuilder,
                          andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder) {
        Topology topology = buildTopology(envProps, avroBuilder, apiAvroBuilder);
        Properties streamProps = buildStreamsProperties(envProps, uri);
        streams = new KafkaStreams(topology, streamProps);
    }

    private Topology buildTopology(Properties envProps,
                                   AvroBuilder avroBuilder,
                                   andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String apiTopicName = envProps.getProperty("friendsdrinks-api.topic.name");

        KStream<String, andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> apiEvents =
                builder.stream(apiTopicName,
                        Consumed.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));

        final String frontendPrivateTopicName = envProps.getProperty("frontend-private.topic.name");
        buildResponsesStore(builder, apiEvents, apiAvroBuilder, frontendPrivateTopicName);

        final String friendsDrinksStateTopicName = envProps.getProperty("friendsdrinks-state.topic.name");
        KStream<FriendsDrinksId, FriendsDrinksState> friendsDrinksState =
                builder.stream(friendsDrinksStateTopicName,
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        friendsDrinksState.toTable(
                Materialized.<FriendsDrinksId, FriendsDrinksState, KeyValueStore<Bytes, byte[]>>
                        as(FRIENDSDRINKS_STORE)
                        .withKeySerde(avroBuilder.friendsDrinksIdSerde())
                        .withValueSerde(avroBuilder.friendsDrinksStateSerde()));

        friendsDrinksState.selectKey((key, value) -> key.getUuid())
                .toTable(
                        Materialized.<String, FriendsDrinksState, KeyValueStore<Bytes, byte[]>>
                                as(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(avroBuilder.friendsDrinksStateSerde()));


        final String pendingInvitationsTopicName = envProps.getProperty("friendsdrinks-pending-invitation.topic.name");
        builder.table(pendingInvitationsTopicName,
                Consumed.with(apiAvroBuilder.friendsDrinksPendingInvitationIdSerde(), apiAvroBuilder.friendsDrinksPendingInvitationSerde()),
                Materialized.as(PENDING_INVITATIONS_STORE));

        return builder.build();
    }

    private void buildResponsesStore(StreamsBuilder builder,
                                     KStream<String, andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> stream,
                                     andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                     String responsesTopicName) {
        stream.filter(((key, value) -> {
            EventType eventType = value.getEventType();
            return eventType.equals(EventType.CREATE_FRIENDSDRINKS_RESPONSE) ||
                    eventType.equals(EventType.UPDATE_FRIENDSDRINKS_RESPONSE) ||
                    eventType.equals(EventType.FRIENDSDRINKS_INVITATION_RESPONSE) ||
                    eventType.equals(EventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) ||
                    eventType.equals(EventType.DELETE_FRIENDSDRINKS_RESPONSE);
        })).to(responsesTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));

        KStream<String, FriendsDrinksEvent> responsesStream =
                builder.stream(responsesTopicName, Consumed.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));

        // KTable for getting response results.
        responsesStream.toTable(Materialized.<String, FriendsDrinksEvent, KeyValueStore<Bytes, byte[]>>
                as(RESPONSES_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(apiAvroBuilder.apiFriendsDrinksSerde()));

        // Logic to tombstone responses in responsesTopicName so the KTable doesn't grow indefinitely.
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RequestsPurger.RESPONSES_PENDING_DELETION),
                Serdes.String(),
                apiAvroBuilder.apiFriendsDrinksSerde());
        builder.addStateStore(storeBuilder);
        responsesStream.transform(() -> new RequestsPurger(), RequestsPurger.RESPONSES_PENDING_DELETION)
                .filter((key, value) -> value != null && !value.isEmpty()).flatMapValues(value -> value)
                .selectKey((key, value) -> value).mapValues(value -> (FriendsDrinksEvent) null)
                .to(responsesTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));
    }

    private static Properties buildStreamsProperties(Properties envProps, String uri) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("frontend-restapi-application.id"));
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
