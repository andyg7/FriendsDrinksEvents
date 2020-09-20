package andrewgrant.friendsdrinks.frontend.restapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

import andrewgrant.friendsdrinks.FriendsDrinksAvro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    public static final String CREATE_FRIENDSDRINKS_RESPONSES_STORE = "create-friendsdrinks-responses-store";
    public static final String DELETE_FRIENDSDRINKS_RESPONSES_STORE = "delete-friendsdrinks-responses-store";
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
        final String frontendPrivate1TopicName = envProps.getProperty("frontendPrivate1.topic.name");
        buildCreateFriendsDrinksResponsesStore(builder, apiEvents, friendsDrinksAvro, frontendPrivate1TopicName);;

        final String frontendPrivate2TopicName = envProps.getProperty("frontendPrivate2.topic.name");
        buildDeleteFriendsDrinksResponsesStore(builder, apiEvents, friendsDrinksAvro, frontendPrivate2TopicName);;

        final String currFriendsDrinksTopicName = envProps.getProperty("currFriendsdrinks.topic.name");
        builder.table(currFriendsDrinksTopicName,
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

    private void buildDeleteFriendsDrinksResponsesStore(StreamsBuilder builder,
                                                        KStream<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId,
                                                                andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> stream,
                                                        FriendsDrinksAvro avro,
                                                        String topicName) {
        stream.filter(((key, value) -> value.getEventType().equals(andrewgrant.friendsdrinks.api.avro.EventType.DELETE_FRIENDS_DRINKS_RESPONSE)))
                .mapValues(value -> value.getDeleteFriendsDrinksResponse())
                .selectKey((key, value) -> value.getRequestId())
                .groupByKey(Grouped.with(Serdes.String(), avro.deleteFriendsDrinksResponseSerde()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)).advanceBy(Duration.ofMillis(10)))
                .reduce((value1, value2) -> value1)
                .toStream((key, value) -> key.key())
                .to(topicName, Produced.with(Serdes.String(), avro.deleteFriendsDrinksResponseSerde()));
        builder.table(topicName,
                Consumed.with(Serdes.String(), avro.deleteFriendsDrinksResponseSerde()),
                Materialized.as(DELETE_FRIENDSDRINKS_RESPONSES_STORE));
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
