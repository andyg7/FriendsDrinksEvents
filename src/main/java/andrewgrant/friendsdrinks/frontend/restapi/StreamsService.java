package andrewgrant.friendsdrinks.frontend.restapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

import andrewgrant.friendsdrinks.email.EmailAvro;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.EventType;
import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    public static final String CREATE_USER_REQUESTS_STORE = "create-user-requests-store";
    public static final String DELETE_USER_REQUESTS_STORE = "delete-user-requests-store";
    public static final String EMAILS_STORE = "emails-store";
    private KafkaStreams streams;

    public StreamsService(Properties envProps,
                          String uri,
                          UserAvro userAvro,
                          EmailAvro emailAvro) {
        Topology topology = buildTopology(envProps, userAvro, emailAvro);
        Properties streamProps = buildStreamsProperties(envProps, uri);
        streams = new KafkaStreams(topology, streamProps);
    }

    private Topology buildTopology(Properties envProps,
                                   UserAvro userAvro,
                                   EmailAvro emailAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopicName = envProps.getProperty("user.topic.name");
        KStream<UserId, UserEvent> userEventKStream = builder.stream(userTopicName,
                Consumed.with(userAvro.userIdSerde(), userAvro.userEventSerde()));

        final String frontendPrivate1TopicName = envProps.getProperty("frontendPrivate1.topic.name");
        buildCreateUserRequestsStore(builder, userEventKStream, userAvro, frontendPrivate1TopicName);

        final String frontendPrivate2TopicName = envProps.getProperty("frontendPrivate2.topic.name");
        buildDeleteUserRequestsStore(builder, userEventKStream, userAvro, frontendPrivate2TopicName);

        final String currEmailTopicName = envProps.getProperty("currEmail.topic.name");
        builder.table(currEmailTopicName, emailAvro.consumedWith(), Materialized.as(EMAILS_STORE));

        return builder.build();
    }

    private void buildCreateUserRequestsStore(StreamsBuilder builder,
                                              KStream<UserId, UserEvent> userEventKStream,
                                              UserAvro userAvro,
                                              String privateTopicName) {
        SessionWindows sessionWindows = SessionWindows.with(Duration.ofSeconds(5));
        final long tenMinutesInMs = 1000 * 60 * 1;
        sessionWindows = sessionWindows.until(tenMinutesInMs);
        userEventKStream.filter(((key, value) ->
                value.getEventType().equals(EventType.CREATE_USER_RESPONSE)))
                .mapValues(value -> value.getCreateUserResponse())
                .selectKey((key, value) -> value.getRequestId())
                .groupByKey(Grouped.with(
                        Serdes.String(),
                        userAvro.createUserResponseSerde()))
                .windowedBy(sessionWindows)
                .reduce((value1, value2) -> value1)
                .toStream((key, value) -> key.key())
                .to(privateTopicName, Produced.with(Serdes.String(),
                        userAvro.createUserResponseSerde()));
        builder.table(privateTopicName,
                Consumed.with(Serdes.String(), userAvro.createUserResponseSerde()),
                Materialized.as(CREATE_USER_REQUESTS_STORE));
    }

    private void buildDeleteUserRequestsStore(StreamsBuilder builder,
                                              KStream<UserId, UserEvent> userEventKStream,
                                              UserAvro userAvro,
                                              String privateTopicName) {
        SessionWindows sessionWindows = SessionWindows.with(Duration.ofSeconds(5));
        final long tenMinutesInMs = 1000 * 60 * 1;
        sessionWindows = sessionWindows.until(tenMinutesInMs);
        userEventKStream.filter(((key, value) ->
                value.getEventType().equals(EventType.DELETE_USER_RESPONSE)))
                .mapValues(value -> value.getDeleteUserResponse())
                .selectKey((key, value) -> value.getRequestId())
                .groupByKey(Grouped.with(
                        Serdes.String(),
                        userAvro.deleteUserResponseSerde()))
                .windowedBy(sessionWindows)
                .reduce((value1, value2) -> value1)
                .toStream((key, value) -> key.key())
                .to(privateTopicName, Produced.with(Serdes.String(),
                        userAvro.deleteUserResponseSerde()));
        builder.table(privateTopicName,
                Consumed.with(Serdes.String(), userAvro.deleteUserResponseSerde()),
                Materialized.as(DELETE_USER_REQUESTS_STORE));
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
