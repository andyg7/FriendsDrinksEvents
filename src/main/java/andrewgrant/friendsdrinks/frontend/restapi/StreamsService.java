package andrewgrant.friendsdrinks.frontend.restapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

import andrewgrant.friendsdrinks.FriendsDrinksAvro;
import andrewgrant.friendsdrinks.api.avro.EventType;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    public static final String RESPONSES_STORE = "responses-store";
    public static final String FRIENDSDRINKS_STORE = "friendsdrinks-store";
    private KafkaStreams streams;

    public StreamsService(Properties envProps,
                          String uri,
                          FriendsDrinksAvro friendsDrinksAvro) {
        Topology topology = buildTopology(envProps, friendsDrinksAvro);
        Properties streamProps = buildStreamsProperties(envProps, uri);
        streams = new KafkaStreams(topology, streamProps);
    }

    private Topology buildTopology(Properties envProps,
                                   FriendsDrinksAvro friendsDrinksAvro) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String apiTopicName = envProps.getProperty("friendsdrinks_api.topic.name");

        KStream<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId, andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> apiEvents =
                builder.stream(apiTopicName,
                        Consumed.with(friendsDrinksAvro.apiFriendsDrinksIdSerde(), friendsDrinksAvro.apiFriendsDrinksSerde()));

        final String frontendPrivateTopicName = envProps.getProperty("frontend_private.topic.name");
        buildResponsesStore(builder, apiEvents, friendsDrinksAvro, frontendPrivateTopicName);;

        final String friendsDrinksStateTopicName = envProps.getProperty("friendsdrinks_state.topic.name");
        builder.table(friendsDrinksStateTopicName,
                Consumed.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksStateSerde()),
                Materialized.as(FRIENDSDRINKS_STORE));

        return builder.build();
    }

    private void buildResponsesStore(StreamsBuilder builder,
                                     KStream<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId,
                                             andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> stream,
                                     FriendsDrinksAvro friendsDrinksAvro,
                                     String responsesTopicName) {
        stream.filter(((key, value) -> {
            EventType eventType = value.getEventType();
            return eventType.equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE) ||
                    eventType.equals(EventType.UPDATE_FRIENDS_DRINKS_RESPONSE) ||
                    eventType.equals(EventType.DELETE_FRIENDS_DRINKS_RESPONSE);
        }))
                .selectKey((key, value) -> {
                    if (value.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE)) {
                        return value.getCreateFriendsDrinksResponse().getRequestId();
                    } else if (value.getEventType().equals(EventType.UPDATE_FRIENDS_DRINKS_RESPONSE)) {
                        return value.getUpdateFriendsDrinksResponse().getRequestId();
                    } else if (value.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_RESPONSE)) {
                        return value.getDeleteFriendsDrinksResponse().getRequestId();
                    } else {
                        throw new RuntimeException(String.format("Unknown event type %s", value.getEventType().toString()));
                    }
                }).to(responsesTopicName, Produced.with(Serdes.String(), friendsDrinksAvro.apiFriendsDrinksSerde()));
        // KTable for getting response results.
        builder.table(responsesTopicName, Consumed.with(Serdes.String(), friendsDrinksAvro.apiFriendsDrinksSerde()),
                Materialized.as(RESPONSES_STORE));

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RequestsPurger.RESPONSES_PENDING_DELETION),
                Serdes.String(),
                friendsDrinksAvro.apiFriendsDrinksSerde());
        builder.addStateStore(storeBuilder);


        builder.stream(responsesTopicName, Consumed.with(Serdes.String(), friendsDrinksAvro.apiFriendsDrinksSerde()))
                .transform(() -> new RequestsPurger(), RequestsPurger.RESPONSES_PENDING_DELETION)
                .filter((key, value) -> value != null).flatMapValues(value -> value)
                .selectKey((key, value) -> value).mapValues(value -> (FriendsDrinksEvent) null)
                .to(responsesTopicName, Produced.with(Serdes.String(), friendsDrinksAvro.apiFriendsDrinksSerde()));
    }

    private static Properties buildStreamsProperties(Properties envProps, String uri) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("frontend_application.id"));
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
