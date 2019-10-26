package andrewgrant.friendsdrinks.email;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
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

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.EmailEvent;
import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import andrewgrant.friendsdrinks.user.UserAvroSerdeFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * Service for validating requests based on email.
 */
public class UserEmailValidatorService {
    private static final Logger log = LoggerFactory.getLogger(UserEmailValidatorService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

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

        final String emailTopic = envProps.getProperty("email.topic.name");
        // Get stream of email events and clean up state store as they come in
        KStream<String, Email> emailKStream = builder.stream(emailTopic,
                Consumed.with(Serdes.String(), EmailAvroSerdeFactory.build(envProps)))
                .transform(PendingEmailsStateStoreCleaner::new, PENDING_EMAILS_STORE_NAME);

        // Write events to a tmp topic so we can rebuild a table
        final String emailTmpTopic = envProps.getProperty("email_tmp.topic.name");
        emailKStream.filter(((key, value) -> value.getEventType().equals(EmailEvent.RESERVED)))
                .to(emailTmpTopic,
                        Produced.with(Serdes.String(),
                                EmailAvroSerdeFactory.build(envProps)));
        KTable<String, Email> emailKTable = builder.table(emailTmpTopic);

        final String userTopic = envProps.getProperty("user.topic.name");
        KStream<String, User> userIdKStream = builder.stream(userTopic);
        // Filter by requests.
        KStream<String, User> userRequestsKStream = userIdKStream.
                filter(((key, value) -> value.getEventType().equals(UserEvent.REQUESTED)));
        // Key by email so we can join on email.
        KStream<String, User> userKStream = userRequestsKStream
                .selectKey(((key, value) -> value.getEmail()));

        KStream<String, EmailRequest> userAndEmail = userKStream.leftJoin(emailKTable,
                EmailRequest::new,
                Joined.with(Serdes.String(),
                        UserAvroSerdeFactory.build(envProps),
                        EmailAvroSerdeFactory.build(envProps)));

        KStream<String, User> validatedUser =
                userAndEmail.transform(EmailValidator::new, PENDING_EMAILS_STORE_NAME);

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        validatedUser.to(userValidationTopic,
                Produced.with(Serdes.String(),
                        UserAvroSerdeFactory.build(envProps)));

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
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        UserEmailValidatorService userEmailValidatorService = new UserEmailValidatorService();
        Properties envProps = userEmailValidatorService.loadEnvProperties(args[0]);
        Topology topology = userEmailValidatorService.buildTopology(envProps);
        log.debug("Built stream");

        Properties streamProps = userEmailValidatorService.buildStreamsProperties(envProps);
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
