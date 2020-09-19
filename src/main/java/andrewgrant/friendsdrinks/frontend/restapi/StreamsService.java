package andrewgrant.friendsdrinks.frontend.restapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

import andrewgrant.friendsdrinks.FriendsDrinksAvro;
import andrewgrant.friendsdrinks.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    public static final String CREATE_FRIENDSDRINKS_RESPONSES_STORE = "create-friendsdrinks-responses-store";
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


        final String friendsDrinksApiTopicName = envProps.getProperty("friendsdrinks_api.topic.name");
        KStream<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId, andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> friendsDrinksApiKStream =
                builder.stream(friendsDrinksApiTopicName,
                        Consumed.with(friendsDrinksAvro.apiFriendsDrinksIdSerde(), friendsDrinksAvro.apiFriendsDrinksSerde()));
        final String frontendPrivate3TopicName = envProps.getProperty("frontendPrivate3.topic.name");
        buildCreateFriendsDrinksResponsesStore(builder, friendsDrinksApiKStream, friendsDrinksAvro, frontendPrivate3TopicName);;

        final String friendsDrinksTopicName = envProps.getProperty("friendsdrinks.topic.name");
        KStream<FriendsDrinksId, FriendsDrinksEvent> friendsDrinksEventStream =
                builder.stream(friendsDrinksTopicName,
                        Consumed.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()))
                .mapValues((value -> {
                    if (value.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.CREATED)) {
                        return value;
                    } else if (value.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.DELETED)) {
                        // Tombstone deleted friends drinks.
                        return null;
                    } else {
                        throw new RuntimeException(String.format("Unknown event type %s", value.getEventType().toString()));
                    }
                }));
        final String currFriendsDrinksTopicName = envProps.getProperty("currFriendsdrinks.topic.name");
        friendsDrinksEventStream.to(currFriendsDrinksTopicName,
                Produced.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()));
        builder.table(
                currFriendsDrinksTopicName,
                Consumed.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()),
                Materialized.as(FRIENDSDRINKS_STORE));

        return builder.build();
    }

    private void buildCreateFriendsDrinksResponsesStore(StreamsBuilder builder,
                                                        KStream<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId,
                                                                andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> stream,
                                                        FriendsDrinksAvro avro,
                                                        String topicName) {
        stream.filter(((key, value) -> value.getEventType().equals(andrewgrant.friendsdrinks.api.avro.EventType.CREATE_FRIENDS_DRINKS_RESPONSE)))
                .mapValues(value -> value.getCreateFriendsDrinksResponse())
                .selectKey((key, value) -> value.getRequestId())
                .groupByKey(Grouped.with(Serdes.String(), avro.createFriendsDrinksResponseSerde()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)).advanceBy(Duration.ofMillis(10)))
                .reduce((value1, value2) -> value1)
                .toStream((key, value) -> key.key())
                .to(topicName, Produced.with(Serdes.String(),
                        avro.createFriendsDrinksResponseSerde()));
        builder.table(topicName,
                Consumed.with(Serdes.String(), avro.createFriendsDrinksResponseSerde()),
                Materialized.as(CREATE_FRIENDSDRINKS_RESPONSES_STORE));
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
