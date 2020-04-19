package andrewgrant.friendsdrinks;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import andrewgrant.friendsdrinks.api.avro.CreateFriendsDrinksRequest;
import andrewgrant.friendsdrinks.api.avro.CreateFriendsDrinksResponse;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksApi;
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

    public SpecificAvroSerde<FriendsDrinksApi> friendsDrinksApiSerde() {
        SpecificAvroSerde<FriendsDrinksApi> serde;
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

    public Serializer<FriendsDrinksApi> friendsDrinksApiSerializer() {
        SpecificAvroSerde<FriendsDrinksApi> serde = friendsDrinksApiSerde();
        return serde.serializer();
    }

    public Serializer<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> apiFriendsDrinksIdSerializer() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> serde = apiFriendsDrinksIdSerde();
        return serde.serializer();
    }

    public Deserializer<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> friendsDrinksIdDeserializer() {
        return apiFriendsDrinksIdSerde().deserializer();
    }

    public Deserializer<FriendsDrinksApi> friendsDrinksApiDeserializer() {
        SpecificAvroSerde<FriendsDrinksApi> serde = friendsDrinksApiSerde();
        return serde.deserializer();
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

    public SpecificAvroSerde<andrewgrant.friendsdrinks.avro.FriendsDrinksId> friendsDrinksIdSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.avro.FriendsDrinksId> serde;
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

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> apiFriendsDrinksIdSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> serde;
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
}
