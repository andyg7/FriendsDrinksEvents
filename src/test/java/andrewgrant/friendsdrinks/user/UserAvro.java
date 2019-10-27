package andrewgrant.friendsdrinks.user;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserId;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Class for building User serializer and deserializer.
 */
public class UserAvro {

    public static SpecificAvroSerializer<User> userSerializer(Properties envProps) {
        SpecificAvroSerializer<User> serializer = new SpecificAvroSerializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        serializer.configure(config, false);
        return serializer;
    }

    public static SpecificAvroSerializer<UserId> userIdSerializer(Properties envProps) {
        SpecificAvroSerializer<UserId> serializer = new SpecificAvroSerializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        serializer.configure(config, true);
        return serializer;
    }

    public static SpecificAvroDeserializer<User> userDeserializer(Properties envProps) {
        SpecificAvroDeserializer<User> deserializer = new SpecificAvroDeserializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        deserializer.configure(config, false);
        return deserializer;
    }

    public static SpecificAvroDeserializer<UserId> userIdDeserializer(Properties envProps) {
        SpecificAvroDeserializer<UserId> deserializer = new SpecificAvroDeserializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        deserializer.configure(config, true);
        return deserializer;
    }
}
