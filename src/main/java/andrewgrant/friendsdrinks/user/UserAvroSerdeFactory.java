package andrewgrant.friendsdrinks.user;

import java.util.HashMap;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserId;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * Factory for building a avro encoder/decoder of User.
 */
public class UserAvroSerdeFactory {

    public static SpecificAvroSerde<User> buildUser(Properties properties) {
        SpecificAvroSerde<User> serde = new SpecificAvroSerde<>();

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
