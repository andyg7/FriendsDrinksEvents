package andrewgrant.friendsdrinks.user;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashMap;
import java.util.Map;

import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;


/**
 * Class for building serialization related for user topic.
 */
public class UserAvro {

    private String registryUrl;
    private SchemaRegistryClient registryClient;

    public UserAvro(String registryUrl, SchemaRegistryClient registryClient) {
        this.registryUrl = registryUrl;
        this.registryClient = registryClient;
    }

    public UserAvro(String registryUrl) {
        this.registryUrl = registryUrl;
        this.registryClient = null;
    }

    public Serializer<UserEvent> userEventSerializer() {
        SpecificAvroSerde<UserEvent> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, false);
        return serde.serializer();
    }

    public SpecificAvroSerde<UserEvent> userEventSerde() {
        SpecificAvroSerde<UserEvent> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public Serializer<UserId> userIdSerializer() {
        SpecificAvroSerde<UserId> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, true);
        return serde.serializer();
    }

    public SpecificAvroSerde<UserId> userIdSerde() {
        SpecificAvroSerde<UserId> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, true);
        return serde;
    }

    public SpecificAvroSerde<CreateUserRequest> createUserRequestSerde() {
        SpecificAvroSerde<CreateUserRequest> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<CreateUserResponse> createUserResponseSerde() {
        SpecificAvroSerde<CreateUserResponse> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<DeleteUserRequest> deleteUserRequestSerde() {
        SpecificAvroSerde<DeleteUserRequest> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<DeleteUserResponse> deleteUserResponseSerde() {
        SpecificAvroSerde<DeleteUserResponse> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public Deserializer<UserEvent> userEventDeserializer() {
        SpecificAvroSerde<UserEvent> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
           serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, false);
        return serde.deserializer();
    }

    public Deserializer<UserId> userIdDeserializer() {
        SpecificAvroSerde<UserId> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                registryUrl);
        serde.configure(config, true);
        return serde.deserializer();
    }

    public Consumed<UserId, UserEvent> consumedWith() {
        return Consumed.with(userIdSerde(), userEventSerde());
    }

    public Produced<UserId, UserEvent> producedWith() {
        return Produced.with(userIdSerde(), userEventSerde());
    }

}
