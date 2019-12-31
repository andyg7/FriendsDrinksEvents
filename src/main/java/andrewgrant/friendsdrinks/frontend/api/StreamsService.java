package andrewgrant.friendsdrinks.frontend.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.time.Duration;
import java.util.Properties;

import andrewgrant.friendsdrinks.email.EmailAvro;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.CreateUserResponse;
import andrewgrant.friendsdrinks.user.avro.EventType;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    private static final String requestsStore = "requests-store";
    private static final String emailsStore = "emails-store";
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

        SessionWindows sessionWindows = SessionWindows.with(Duration.ofMinutes(1));
        final long tenMinutesInMs = 1000 * 60 * 10;
        sessionWindows = sessionWindows.until(tenMinutesInMs);
        final String userTopicName = envProps.getProperty("user.topic.name");
        final String frontendPrivate1TopicName =
                envProps.getProperty("frontendPrivate1.topic.name");
        builder.stream(userTopicName,
                userAvro.consumedWith())
                .filter(((key, value) ->
                        value.getEventType().equals(EventType.CREATE_USER_RESPONSE)))
                .mapValues(value -> value.getCreateUserResponse())
                .selectKey((key, value) -> value.getRequestId())
                .groupByKey(Grouped.with(
                        Serdes.String(),
                        userAvro.createUserResponseSerde()))
                .windowedBy(sessionWindows)
                .reduce((value1, value2) -> value1)
                .toStream((key, value) -> key.key())
                .to(frontendPrivate1TopicName, Produced.with(Serdes.String(),
                        userAvro.createUserResponseSerde()));
        builder.table(frontendPrivate1TopicName,
                Consumed.with(Serdes.String(), userAvro.createUserResponseSerde()),
                Materialized.as(requestsStore));


        final String emailPrivate4TopicName =
                envProps.getProperty("emailPrivate4.topic.name");
        final String emailTopicName = envProps.getProperty("email.topic.name");
        builder.stream(emailTopicName, emailAvro.consumedWith())
                .filter(((key, value) ->
                        value.getEventType().equals(
                                andrewgrant.friendsdrinks.email.avro.EventType.RESERVED) ||
                                value.getEventType().equals(
                                        andrewgrant.friendsdrinks.email.avro.EventType.RETURNED)))
                .mapValues(value -> {
                    if (value.getEventType().equals(
                            andrewgrant.friendsdrinks.email.avro.EventType.RETURNED)) {
                        return null;
                    } else {
                        return value.getUserId();
                    }
                })
                .selectKey(((key, value) -> key.getEmailAddress()))
                .to(emailPrivate4TopicName, Produced.with(Serdes.String(), Serdes.String()));
        builder.table(emailPrivate4TopicName,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as(emailsStore));

        return builder.build();
    }

    private static Properties buildStreamsProperties(Properties envProps, String uri) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("frontend_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, uri);
        return props;
    }

    public ReadOnlyKeyValueStore<String, CreateUserResponse> getRequestsKvStore() {
        return streams.store(requestsStore, QueryableStoreTypes.keyValueStore());
    }

    public ReadOnlyKeyValueStore<String, String> getEmailsKvStore() {
        return streams.store(emailsStore, QueryableStoreTypes.keyValueStore());
    }

    public KafkaStreams getStreams() {
        return streams;
    }

}
