package andrewgrant.friendsdrinks;

import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import andrewgrant.friendsdrinks.api.avro.DeleteFriendsDrinksResponse;
import andrewgrant.friendsdrinks.api.avro.UpsertFriendsDrinksRequest;
import andrewgrant.friendsdrinks.api.avro.UpsertFriendsDrinksResponse;

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

    public SpecificAvroSerde<UpsertFriendsDrinksRequest> upsertFriendsDrinksRequestSerde() {
        SpecificAvroSerde<UpsertFriendsDrinksRequest> serde;
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

    public SpecificAvroSerde<UpsertFriendsDrinksResponse> upsertFriendsDrinksResponseSerde() {
        SpecificAvroSerde<UpsertFriendsDrinksResponse> serde;
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

    public SpecificAvroSerde<DeleteFriendsDrinksResponse> deleteFriendsDrinksResponseSerde() {
        SpecificAvroSerde<DeleteFriendsDrinksResponse> serde;
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

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> apiFriendsDrinksSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> serde;
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

    public Serializer<andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> apiFriendsDrinksSerializer() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> serde = apiFriendsDrinksSerde();
        return serde.serializer();
    }

    public Serializer<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> apiFriendsDrinksIdSerializer() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> serde = apiFriendsDrinksIdSerde();
        return serde.serializer();
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.avro.FriendsDrinksState> friendsDrinksStateSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.avro.FriendsDrinksState> serde;
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

    public SpecificAvroSerde<andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> friendsDrinksEventSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> serde;
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

    public SpecificAvroSerde<andrewgrant.friendsdrinks.avro.FriendsDrinksStateAggregate> friendsDrinksStateAggregateSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.avro.FriendsDrinksStateAggregate> serde;
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

}
