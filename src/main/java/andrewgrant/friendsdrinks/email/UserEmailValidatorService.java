package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import andrewgrant.friendsdrinks.user.UserAvroSerdeFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UserEmailValidatorService {
    private static final Logger log = LoggerFactory.getLogger(UserEmailValidatorService.class);

    private String USER_TOPIC;
    private String USER_VALIDATION_TOPIC;
    private String EMAIL_TOPIC;

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }

    public static final String PENDING_EMAILS_STORE_NAME = "pending_emails_store_name";

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder pendingEmails = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(PENDING_EMAILS_STORE_NAME),
                        Serdes.String(), Serdes.String())
                .withLoggingEnabled(new HashMap<>());
        builder.addStateStore(pendingEmails);

        USER_TOPIC = envProps.getProperty("user.topic.name");
        KStream<String, User> userIdKStream = builder.stream(USER_TOPIC);

        EMAIL_TOPIC = envProps.getProperty("email.topic.name");
        KTable<String, Email> emailKTable = builder.table(EMAIL_TOPIC,
                Consumed.with(Serdes.String(), EmailAvroSerdeFactory.build(envProps)));

        // Filter by requests.
        KStream<String, User> userRequestsKStream = userIdKStream.filter(((key, value) -> value.getEventType()
                .equals(UserEvent.REQUESTED)));
        // Key by email so we can join on email.
        KStream<String, User> userKStream = userRequestsKStream.selectKey(((key, value) -> value.getEmail()));

        KStream<String, EmailRequest> userAndEmail = userKStream.leftJoin(emailKTable, EmailRequest::new,
                Joined.with(Serdes.String(), UserAvroSerdeFactory.build(envProps), EmailAvroSerdeFactory.build(envProps)));

        KStream<String, User> validatedUser = userAndEmail.transform(EmailValidator::new, PENDING_EMAILS_STORE_NAME);
        USER_VALIDATION_TOPIC = envProps.getProperty("user_validation.topic.name");
        validatedUser.to(USER_VALIDATION_TOPIC, Produced.with(Serdes.String(), UserAvroSerdeFactory.build(envProps)));

        return builder.build();
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

        UserEmailValidatorService userEmailValidatorService = new UserEmailValidatorService();
        Properties envProps = userEmailValidatorService.loadEnvProperties(args[0]);
        Properties streamProps = userEmailValidatorService.buildStreamsProperties(envProps);
        Topology topology = userEmailValidatorService.buildTopology(envProps);
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
