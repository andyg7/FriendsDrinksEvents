package andrewgrant.friendsdrinks.email;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Factory class for building an Email Avro serializer and deserializer.
 */
public class EmailAvro {

    public static SpecificAvroSerializer<EmailEvent> emailSerializer(Properties envProps) {
        SpecificAvroSerializer<EmailEvent> serializer = new SpecificAvroSerializer<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        serializer.configure(config, false);
        return serializer;
    }

    public static SpecificAvroSerializer<EmailId> emailIdSerializer(Properties envProps) {
        SpecificAvroSerializer<EmailId> serializer = new SpecificAvroSerializer<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        serializer.configure(config, true);
        return serializer;
    }

    public static SpecificAvroDeserializer<EmailEvent> emailDeserializer(Properties envProps) {
        SpecificAvroDeserializer<EmailEvent> deserializer = new SpecificAvroDeserializer<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        deserializer.configure(config, false);
        return deserializer;
    }

    public static SpecificAvroDeserializer<EmailId> emailIdDeserializer(Properties envProps) {
        SpecificAvroDeserializer<EmailId> deserializer = new SpecificAvroDeserializer<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        deserializer.configure(config, true);
        return deserializer;
    }

}
