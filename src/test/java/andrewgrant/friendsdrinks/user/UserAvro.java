package andrewgrant.friendsdrinks.user;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Class for building User serializer and deserializer.
 */
public class UserAvro {

    public static SpecificAvroSerializer<UserEvent> userEventSerializer(Properties envProps) {
        SpecificAvroSerializer<UserEvent> serializer = new SpecificAvroSerializer<>();
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

    public static SpecificAvroDeserializer<UserEvent> userDeserializer(Properties envProps) {
        SpecificAvroDeserializer<UserEvent> deserializer = new SpecificAvroDeserializer<>();
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
