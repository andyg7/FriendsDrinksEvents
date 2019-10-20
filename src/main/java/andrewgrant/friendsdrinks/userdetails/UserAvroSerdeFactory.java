package andrewgrant.friendsdrinks.userdetails;

import andrewgrant.friendsdrinks.avro.User;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.util.HashMap;
import java.util.Properties;

public class UserAvroSerdeFactory {

    public static SpecificAvroSerde<User> build(Properties properties) {

        SpecificAvroSerde<User> userAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        userAvroSerde.configure(serdeConfig, false);
        return userAvroSerde;
    }
}
