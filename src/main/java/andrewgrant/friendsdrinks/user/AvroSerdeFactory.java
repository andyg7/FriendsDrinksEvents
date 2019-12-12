package andrewgrant.friendsdrinks.user;

import java.util.HashMap;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * Factory for building a avro encoder/decoder of User.
 */
public class AvroSerdeFactory {

    public static SpecificAvroSerde<UserEvent> buildUserEvent(Properties properties) {
        SpecificAvroSerde<UserEvent> serde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        serde.configure(serdeConfig, false);
        return serde;
    }

    public static SpecificAvroSerde<CreateUserRequest> buildCreateUserRequest(
            Properties properties) {
        SpecificAvroSerde<CreateUserRequest> serde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        serde.configure(serdeConfig, false);
        return serde;
    }

    public static SpecificAvroSerde<DeleteUserRequest> buildDeleteUserRequest(
            Properties properties) {
        SpecificAvroSerde<DeleteUserRequest> serde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        serde.configure(serdeConfig, false);
        return serde;
    }

    public static SpecificAvroSerde<DeleteUserResponse> buildDeleteUserResponse(
            Properties properties) {
        SpecificAvroSerde<DeleteUserResponse> serde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        serde.configure(serdeConfig, false);
        return serde;
    }

    public static SpecificAvroSerde<UserId> buildUserId(Properties properties) {
        SpecificAvroSerde<UserId> userAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        userAvroSerde.configure(serdeConfig, true);
        return userAvroSerde;
    }
}
