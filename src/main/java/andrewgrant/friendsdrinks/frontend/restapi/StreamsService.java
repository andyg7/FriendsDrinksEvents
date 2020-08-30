package andrewgrant.friendsdrinks.frontend.restapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

import andrewgrant.friendsdrinks.FriendsDrinksAvro;
import andrewgrant.friendsdrinks.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.email.EmailAvro;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.api.avro.EventType;
import andrewgrant.friendsdrinks.user.api.avro.UserEvent;
import andrewgrant.friendsdrinks.user.api.avro.UserId;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    public static final String CREATE_USER_RESPONSES_STORE = "create-user-responses-store";
    public static final String DELETE_USER_RESPONSES_STORE = "delete-user-responses-store";
    public static final String CREATE_FRIENDSDRINKS_RESPONSES_STORE = "create-friendsdrinks-responses-store";
    public static final String EMAILS_STORE = "emails-store-1";
    public static final String FRIENDSDRINKS_STORE = "friendsdrinks-store";
    private KafkaStreams streams;

    public StreamsService(Properties envProps,
                          String uri,
                          UserAvro userAvro,
                          EmailAvro emailAvro,
                          FriendsDrinksAvro friendsDrinksAvro) {
        Topology topology = buildTopology(envProps, userAvro, emailAvro, friendsDrinksAvro);
        Properties streamProps = buildStreamsProperties(envProps, uri);
        streams = new KafkaStreams(topology, streamProps);
    }

    private Topology buildTopology(Properties envProps,
                                   UserAvro userAvro,
                                   EmailAvro emailAvro,
                                   FriendsDrinksAvro friendsDrinksAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopicName = envProps.getProperty("user_api.topic.name");
        KStream<UserId, UserEvent> userEventKStream = builder.stream(userTopicName,
                Consumed.with(userAvro.userIdSerde(), userAvro.userEventSerde()));

        final String frontendPrivate1TopicName = envProps.getProperty("frontendPrivate1.topic.name");
        buildCreateUserResponsesStore(builder, userEventKStream, userAvro, frontendPrivate1TopicName);

        final String frontendPrivate2TopicName = envProps.getProperty("frontendPrivate2.topic.name");
        buildDeleteUserResponsesStore(builder, userEventKStream, userAvro, frontendPrivate2TopicName);

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
                        return null;
                    } else {
                        throw new RuntimeException(String.format("Unknown event type %s", value.getEventType().toString()));
                    }
                }));
        final String currFriendsDrinksTopicName = envProps.getProperty("currFriendsdrinks.topic.name");
        friendsDrinksEventStream.to(currFriendsDrinksTopicName,
                Produced.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()));
        builder.table(currFriendsDrinksTopicName, emailAvro.consumedWith(), Materialized.as(FRIENDSDRINKS_STORE));

        final String currEmailTopicName = envProps.getProperty("currEmail.topic.name");
        builder.table(currEmailTopicName, emailAvro.consumedWith(), Materialized.as(EMAILS_STORE));

        return builder.build();
    }

    private void buildCreateUserResponsesStore(StreamsBuilder builder,
                                               KStream<UserId, UserEvent> userEventKStream,
                                               UserAvro userAvro,
                                               String privateTopicName) {
        userEventKStream.filter(((key, value) ->
                value.getEventType().equals(EventType.CREATE_USER_RESPONSE)))
                .mapValues(value -> value.getCreateUserResponse())
                .selectKey((key, value) -> value.getRequestId())
                .groupByKey(Grouped.with(Serdes.String(), userAvro.createUserResponseSerde()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).advanceBy(Duration.ofMillis(10)))
                .reduce((value1, value2) -> value1)
                .toStream((key, value) -> key.key())
                .to(privateTopicName, Produced.with(Serdes.String(), userAvro.createUserResponseSerde()));
        builder.table(privateTopicName, Consumed.with(Serdes.String(), userAvro.createUserResponseSerde()),
                Materialized.as(CREATE_USER_RESPONSES_STORE));
    }

    private void buildDeleteUserResponsesStore(StreamsBuilder builder,
                                               KStream<UserId, UserEvent> userEventKStream,
                                               UserAvro userAvro,
                                               String privateTopicName) {
        userEventKStream.filter(((key, value) ->
                value.getEventType().equals(EventType.DELETE_USER_RESPONSE)))
                .mapValues(value -> value.getDeleteUserResponse())
                .selectKey((key, value) -> value.getRequestId())
                .groupByKey(Grouped.with(Serdes.String(), userAvro.deleteUserResponseSerde()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).advanceBy(Duration.ofMillis(10)))
                .reduce((value1, value2) -> value1)
                .toStream((key, value) -> key.key())
                .to(privateTopicName, Produced.with(Serdes.String(),
                        userAvro.deleteUserResponseSerde()));
        builder.table(privateTopicName,
                Consumed.with(Serdes.String(), userAvro.deleteUserResponseSerde()),
                Materialized.as(DELETE_USER_RESPONSES_STORE));
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
