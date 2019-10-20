package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.EmailEvent;
import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EmailService {
    private static final Logger log = LoggerFactory.getLogger(EmailService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }

    public static String USER_VALIDATION_TOPIC;
    public static String EMAIL_TOPIC;

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        USER_VALIDATION_TOPIC = envProps.getProperty("user_validation.topic.name");
        EMAIL_TOPIC = envProps.getProperty("email.topic.name");

        KStream<String, User> userValidations = builder.stream(USER_VALIDATION_TOPIC,
                Consumed.with(Serdes.String(), userAvroSerde(envProps)));

        KStream<String, User> userKStream = userValidations.filter(((key, value) ->
                value.getEventType().equals(UserEvent.REJECTED) ||
                        value.getEventType().equals(UserEvent.VALIDATED)
        ));

        KStream<String, Email> emailKStream = userKStream.mapValues(((key, value) -> {
            if (value.getEventType().equals(UserEvent.VALIDATED)) {
                Email email = new Email();
                email.setRequestId(value.getRequestId());
                email.setEmail(value.getEmail());
                email.setEventType(EmailEvent.RESERVED);
                return email;
            } else if (value.getEventType().equals(UserEvent.REJECTED)) {
                Email email = new Email();
                email.setRequestId(value.getRequestId());
                email.setEmail(value.getEmail());
                email.setEventType(EmailEvent.RECLAIMED);
                return email;
            } else {
                throw new RuntimeException(String.format("Encountered unknown event type: %s",
                        value.getEventType().toString()));
            }
        }));

        emailKStream.to(EMAIL_TOPIC, Produced.with(Serdes.String(), emailAvroSerde(envProps)));

        return builder.build();
    }

    private SpecificAvroSerde<Email> emailAvroSerde(Properties envProps) {
        SpecificAvroSerde<Email> emailAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        emailAvroSerde.configure(serdeConfig, false);
        return emailAvroSerde;
    }

    private SpecificAvroSerde<User> userAvroSerde(Properties envProps) {
        SpecificAvroSerde<User> userAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        userAvroSerde.configure(serdeConfig, false);
        return userAvroSerde;
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        EmailService emailService = new EmailService();
        Properties envProps = emailService.loadEnvProperties(args[0]);
        Properties streamProps = emailService.buildStreamsProperties(envProps);
        Topology topology = emailService.buildTopology(envProps);
        log.debug("Built stream");

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
