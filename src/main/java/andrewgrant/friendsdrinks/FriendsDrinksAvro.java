package andrewgrant.friendsdrinks;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import andrewgrant.friendsdrinks.avro.CreateFriendsDrinksRequest;
import andrewgrant.friendsdrinks.avro.CreateFriendsDrinksResponse;
import andrewgrant.friendsdrinks.avro.FriendsDrinksEvent;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * Contains (de)serialization logic for friends drinks avros.
 */
public class FriendsDrinksAvro {

    private SchemaRegistryClient registryClient;
    private String registryUrl;

    public FriendsDrinksAvro(String registryUrl) {
        this.registryUrl = registryUrl;
        registryClient = null;
    }

    public FriendsDrinksAvro(String registryUrl, SchemaRegistryClient registryClient) {
        this.registryUrl = registryUrl;
        this.registryClient = registryClient;
    }

    public SpecificAvroSerde<CreateFriendsDrinksRequest> createFriendsDrinksRequestSerde() {
        SpecificAvroSerde<CreateFriendsDrinksRequest> serde;
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

    public SpecificAvroSerde<CreateFriendsDrinksResponse> createFriendsDrinksResponseSerde() {
        SpecificAvroSerde<CreateFriendsDrinksResponse> serde;
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

    public SpecificAvroSerde<FriendsDrinksEvent> friendsDrinksEventSerde() {
        SpecificAvroSerde<FriendsDrinksEvent> serde;
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

    public Serializer<FriendsDrinksEvent> friendsDrinksEventSerializer() {
        SpecificAvroSerde<FriendsDrinksEvent> serde = friendsDrinksEventSerde();
        return serde.serializer();
    }

    public Deserializer<FriendsDrinksEvent> friendsDrinksEventDeserializer() {
        SpecificAvroSerde<FriendsDrinksEvent> serde = friendsDrinksEventSerde();
        return serde.deserializer();
    }
}
