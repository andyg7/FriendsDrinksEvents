package andrewgrant.friendsdrinks.email;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * Factory class for building an Email Avro serializer and deserializer.
 */
public class EmailAvro {

    private Properties envProps;
    private SchemaRegistryClient registryClient;

    public EmailAvro(Properties envProps, SchemaRegistryClient registryClient) {
        this.envProps = envProps;
        this.registryClient = registryClient;
    }

    public Serializer<EmailEvent> emailSerializer() {
        SpecificAvroSerde<EmailEvent> serde = new SpecificAvroSerde<>(registryClient);
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        serde.configure(config, false);
        return serde.serializer();
    }

    public SpecificAvroSerde<EmailEvent> emailSerde() {
        SpecificAvroSerde<EmailEvent> serde = new SpecificAvroSerde<>(registryClient);
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<EmailId> emailIdSerde() {
        SpecificAvroSerde<EmailId> serde = new SpecificAvroSerde<>(registryClient);
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        serde.configure(config, true);
        return serde;
    }

    public Serializer<EmailId> emailIdSerializer() {
        SpecificAvroSerde<EmailId> serde = new SpecificAvroSerde<>(registryClient);
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        serde.configure(config, true);
        return serde.serializer();
    }

    public Deserializer<EmailEvent> emailDeserializer() {
        SpecificAvroSerde<EmailEvent> serde = new SpecificAvroSerde<>(registryClient);
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        serde.configure(config, false);
        return serde.deserializer();
    }

    public Deserializer<EmailId> emailIdDeserializer() {
        SpecificAvroSerde<EmailId> serde = new SpecificAvroSerde<>(registryClient);
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));
        serde.configure(config, true);
        return serde.deserializer();
    }

}
