package andrewgrant.friendsdrinks.user;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.User;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Class for building User serializer and deserializer.
 */
public class UserAvro {

    public static SpecificAvroSerializer<User> serializer(Properties envProps) {
        SpecificAvroSerializer<User> serializer = new SpecificAvroSerializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        serializer.configure(config, false);
        return serializer;
    }

    public static SpecificAvroDeserializer<User> deserializer(Properties envProps) {
        SpecificAvroDeserializer<User> deserializer = new SpecificAvroDeserializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        deserializer.configure(config, false);
        return deserializer;
    }
}
