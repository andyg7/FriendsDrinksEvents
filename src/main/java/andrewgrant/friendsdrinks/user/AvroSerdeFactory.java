package andrewgrant.friendsdrinks.user;

import java.util.HashMap;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.UserEvent;
import andrewgrant.friendsdrinks.avro.UserId;
import andrewgrant.friendsdrinks.avro.UserRequest;
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

    public static SpecificAvroSerde<UserRequest> buildUserRequest(Properties properties) {
        SpecificAvroSerde<UserRequest> serde = new SpecificAvroSerde<>();

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
