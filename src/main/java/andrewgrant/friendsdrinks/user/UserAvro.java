package andrewgrant.friendsdrinks.user;

import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;


/**
 * Holds avro related code.
 */
public class UserAvro {

    private SchemaRegistryClient registryClient;
    private String registryUrl;

    public UserAvro(String registryUrl) {
        this.registryUrl = registryUrl;
        registryClient = null;
    }

    public UserAvro(String registryUrl, SchemaRegistryClient registryClient) {
        this.registryUrl = registryUrl;
        this.registryClient = registryClient;
    }

    public SpecificAvroSerde<UserEvent> userEventSerde() {
        SpecificAvroSerde<UserEvent> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<UserId> userIdSerde() {
        SpecificAvroSerde<UserId> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, true);
        return serde;
    }

    public Serializer<UserEvent> userEventSerializer() {
        return userEventSerde().serializer();
    }

    public Serializer<UserId> userIdSerializer() {
        return userIdSerde().serializer();
    }
}
