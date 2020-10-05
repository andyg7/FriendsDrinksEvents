package andrewgrant.friendsdrinks.membership;

import java.util.HashMap;
import java.util.Map;

import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipEvent;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId;

import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksUserAdded;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksUserRemoved;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * Builds serdes.
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

    public SpecificAvroSerde<FriendsDrinksMembershipEvent> friendsDrinksMembershipEventSerdes() {
        SpecificAvroSerde<FriendsDrinksMembershipEvent> serde;
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


    public SpecificAvroSerde<FriendsDrinksMembershipId> friendsDrinksMembershipIdSerdes() {
        SpecificAvroSerde<FriendsDrinksMembershipId> serde;
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

    public SpecificAvroSerde<FriendsDrinksUserAdded> friendsDrinksUserAddedSerdes() {
        SpecificAvroSerde<FriendsDrinksUserAdded> serde;
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

    public SpecificAvroSerde<FriendsDrinksUserRemoved> friendsDrinksUserRemovedSerdes() {
        SpecificAvroSerde<FriendsDrinksUserRemoved> serde;
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
