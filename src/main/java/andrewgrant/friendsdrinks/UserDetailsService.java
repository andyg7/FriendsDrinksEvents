package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.email_state;
import andrewgrant.friendsdrinks.avro.user_event_type;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UserDetailsService {
    private static final Logger log = LoggerFactory.getLogger(UserDetailsService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }

    public static String USER_TOPIC;
    public static String USER_VALIDATION_TOPIC;
    public static String EMAIL_TOPIC;
    public static String EMAIL_STATE_STORE = "email-state-store";

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        USER_TOPIC = envProps.getProperty("user.topic.name");
        USER_VALIDATION_TOPIC = envProps.getProperty("user_validation.topic.name");
        EMAIL_TOPIC = envProps.getProperty("email.topic.name");

        KStream<String, User> userIdKStream = builder.stream(USER_TOPIC);

        KTable<String, Email> emailKTable = builder.table(EMAIL_TOPIC,
                Consumed.with(Serdes.String(), emailAvroSerde(envProps)));

        // Filter by requests.
        KStream<String, User> userRequestsKStream = userIdKStream.filter(((key, value) -> value.getEventType()
                .equals(user_event_type.REQUESTED)));
        // Key by email so we can join on email.
        KStream<String, User> userKStream = userRequestsKStream.selectKey(((key, value) -> value.getEmail()));

        KStream<String, Email> validatedEmail = userKStream.leftJoin(emailKTable, (leftValue, rightValue) -> {
            if (rightValue == null ||
                    rightValue.getEventType().equals(email_state.RECLAIMED)) {
                Email email = new Email();
                email.setRequestId(leftValue.getRequestId());
                email.setEmail(leftValue.getEmail());
                email.setEventType(email_state.RESERVED);
                return email;
            } else {
                Email email = new Email();
                email.setRequestId(leftValue.getRequestId());
                email.setEmail(leftValue.getEmail());
                email.setEventType(email_state.REJECTED);
                return email;
            }
        });

        validatedEmail.to(EMAIL_TOPIC, Produced.with(Serdes.String(), emailAvroSerde(envProps)));

        return builder.build();
    }

    private SpecificAvroSerde<User> userAvroSerde(Properties envProps) {
        SpecificAvroSerde<User> userAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        userAvroSerde.configure(serdeConfig, false);
        return userAvroSerde;
    }

    private SpecificAvroSerde<Email> emailAvroSerde(Properties envProps) {
        SpecificAvroSerde<Email> emailAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        emailAvroSerde.configure(serdeConfig, false);
        return emailAvroSerde;
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

        UserDetailsService userDetailsService = new UserDetailsService();
        Properties envProps = userDetailsService.loadEnvProperties(args[0]);
        Properties streamProps = userDetailsService.buildStreamsProperties(envProps);
        Topology topology = userDetailsService.buildTopology(envProps);
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

    private static class TransformSupplier implements Transformer<String, User, KeyValue<String, User>> {
        private ProcessorContext context;
        private KeyValueStore<String, Email> state;

        public void init(ProcessorContext context) {
            this.context = context;
            this.state = (KeyValueStore<String, Email>) context.getStateStore(EMAIL_STATE_STORE);
            // punctuate each second; can access this.state
        }

        public KeyValue<String, User> transform(String emailAddress, User user) {
            log.debug(String.format("In transform with %s", emailAddress));
            Email email = this.state.get(emailAddress);
            if (email != null) {
                log.debug(String.format("Got email state %s", email.getEventType().toString()));
            } else {
                log.debug("no email yet!");
            }

            if (email == null) {
                email = new Email();
                email.setRequestId(user.getRequestId());
                email.setEmail(emailAddress);
                email.setEventType(email_state.REQUESTED);
                this.state.put(emailAddress, email);
                user.setEventType(user_event_type.VALIDATED);
                return new KeyValue<>(emailAddress, user);
            } else if (email.getEventType().equals(email_state.REQUESTED)) {
                user.setEventType(user_event_type.REJECTED);
                return new KeyValue<>(emailAddress, user);
            } else if (email.getEventType().equals(email_state.RESERVED)) {
                user.setEventType(user_event_type.REJECTED);
                return new KeyValue<>(emailAddress, user);
            } else if (email.getEventType().equals(email_state.RECLAIMED)){
                email.setEventType(email_state.REQUESTED);
                this.state.put(emailAddress, email);
                user.setEventType(user_event_type.VALIDATED);
                return new KeyValue<>(emailAddress, user);
            }
            throw new RuntimeException("Received email event with unknown state: " + email.getEventType().toString());
        }

        public void close() {
            // can access this.state
        }
    }
}
