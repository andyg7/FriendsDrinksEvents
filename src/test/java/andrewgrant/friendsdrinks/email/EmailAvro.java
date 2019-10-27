package andrewgrant.friendsdrinks.email;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.Email;

import andrewgrant.friendsdrinks.avro.EmailId;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Factory class for building an Email Avro serializer and deserializer.
 */
public class EmailAvro {

    public static SpecificAvroSerializer<Email> emailSerializer(Properties envProps) {
        SpecificAvroSerializer<Email> serializer = new SpecificAvroSerializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        serializer.configure(config, false);
        return serializer;
    }

    public static SpecificAvroSerializer<EmailId> emailIdSerializer(Properties envProps) {
        SpecificAvroSerializer<EmailId> serializer = new SpecificAvroSerializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        serializer.configure(config, true);
        return serializer;
    }

    public static SpecificAvroDeserializer<Email> emailDeserializer(Properties envProps) {
        SpecificAvroDeserializer<Email> deserializer = new SpecificAvroDeserializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        deserializer.configure(config, false);
        return deserializer;
    }

    public static SpecificAvroDeserializer<EmailId> emailIdDeserializer(Properties envProps) {
        SpecificAvroDeserializer<EmailId> deserializer = new SpecificAvroDeserializer<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        deserializer.configure(config, true);
        return deserializer;
    }

}
