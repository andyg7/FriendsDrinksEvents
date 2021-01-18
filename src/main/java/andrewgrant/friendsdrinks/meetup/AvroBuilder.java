package andrewgrant.friendsdrinks.meetup;

import java.util.HashMap;
import java.util.Map;

import andrewgrant.friendsdrinks.avro.*;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * Builds avro serdes.
 */
public class AvroBuilder {

    private SchemaRegistryClient registryClient;
    private String registryUrl;

    public AvroBuilder(String registryUrl) {
        this.registryUrl = registryUrl;
        registryClient = null;
    }

    public AvroBuilder(String registryUrl, SchemaRegistryClient registryClient) {
        this.registryUrl = registryUrl;
        this.registryClient = registryClient;
    }

    public SpecificAvroSerde<FriendsDrinksMeetupEvent> friendsDrinksMeetupEventSpecificAvroSerde() {
       SpecificAvroSerde<FriendsDrinksMeetupEvent> serde;
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

    public SpecificAvroSerde<FriendsDrinksMeetupState> friendsDrinksMeetupStateSpecificAvroSerde() {
        SpecificAvroSerde<FriendsDrinksMeetupState> serde;
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

    public SpecificAvroSerde<FriendsDrinksMeetupScheduled> friendsDrinksMeetupScheduledSpecificAvroSerde() {
        SpecificAvroSerde<FriendsDrinksMeetupScheduled> serde;
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

    public SpecificAvroSerde<FriendsDrinksMeetupHappened> friendsDrinksMeetupHappenedSpecificAvroSerde() {
        SpecificAvroSerde<FriendsDrinksMeetupHappened> serde;
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

    public SpecificAvroSerde<FriendsDrinksMeetupId> friendsDrinksMeetupIdSpecificAvroSerde() {
        SpecificAvroSerde<FriendsDrinksMeetupId> serde;
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
