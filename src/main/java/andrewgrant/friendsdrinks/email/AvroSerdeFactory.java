package andrewgrant.friendsdrinks.email;

import java.util.HashMap;
import java.util.Properties;

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;


/**
 * Factory class for building an avro encoder for Email.
 */
public class AvroSerdeFactory {

    public static SpecificAvroSerde<EmailEvent> buildEmail(Properties properties) {
        SpecificAvroSerde<EmailEvent> serde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        serde.configure(serdeConfig, false);
        return serde;
    }

    public static SpecificAvroSerde<EmailId> buildEmailId(Properties properties) {
        SpecificAvroSerde<EmailId> serde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        serde.configure(serdeConfig, true);
        return serde;
    }
}
