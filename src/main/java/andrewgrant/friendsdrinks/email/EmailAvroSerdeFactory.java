package andrewgrant.friendsdrinks.email;

import java.util.HashMap;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.Email;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;


/**
 * Factory class for building an avro encoder for Email.
 */
public class EmailAvroSerdeFactory {

    public static SpecificAvroSerde<Email> build(Properties properties) {
        SpecificAvroSerde<Email> emailAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                properties.getProperty("schema.registry.url"));

        emailAvroSerde.configure(serdeConfig, false);
        return emailAvroSerde;
    }
}
