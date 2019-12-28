package andrewgrant.friendsdrinks.frontend.api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.time.Duration;
import java.util.Properties;

import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.CreateUserResponse;
import andrewgrant.friendsdrinks.user.avro.EventType;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Responsible for building Topology needed by Frontend.
 */
public class StreamsService {

    private static final String requestsStore = "requests-store";
    private KafkaStreams streams;

    public StreamsService(Properties envProps,
                          String uri,
                          UserAvro userAvro) {
        Topology topology = buildTopology(envProps, userAvro);
        Properties streamProps = buildStreamsProperties(envProps, uri);
        streams = new KafkaStreams(topology, streamProps);
    }

    private Topology buildTopology(Properties envProps,
                                          UserAvro userAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        SessionWindows sessionWindows = SessionWindows.with(Duration.ofMinutes(10));
        final long oneMinute = 1000 * 60;
        sessionWindows = sessionWindows.until(oneMinute);
        final String userTopicName = envProps.getProperty("user.topic.name");
        final String frontendPrivateTopicName =
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
                .to(frontendPrivateTopicName, Produced.with(Serdes.String(),
                        userAvro.createUserResponseSerde()));

        builder.table(frontendPrivateTopicName,
                Consumed.with(Serdes.String(), userAvro.createUserResponseSerde()),
                Materialized.as(requestsStore));

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

    public ReadOnlyKeyValueStore<String, CreateUserResponse> getKeyValueStore() {
        return streams.store(requestsStore, QueryableStoreTypes.keyValueStore());
    }

    public KafkaStreams getStreams() {
        return streams;
    }

}
