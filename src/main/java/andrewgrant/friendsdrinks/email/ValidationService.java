package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

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

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.user.AvroSerdeFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;


/**
 * Service for validating requests based on email.
 */
public class ValidationService {
    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("email_validation_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
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
        KStream<EmailId, Email> emailKStream = builder.stream(emailTopic,
                Consumed.with(
                        andrewgrant.friendsdrinks.email.AvroSerdeFactory.buildEmailId(envProps),
                        andrewgrant.friendsdrinks.email.AvroSerdeFactory.buildEmail(envProps)))
                .transform(PendingEmailsStateStoreCleaner::new, PENDING_EMAILS_STORE_NAME);

        // Write events to a tmp topic so we can rebuild a table
        final String emailTmpTopic = envProps.getProperty("email_tmp.topic.name");
        emailKStream.filter(((key, value) -> value.getEventType().equals(EmailEvent.RESERVED)))
                .to(emailTmpTopic,
                        Produced.with(
                                andrewgrant.friendsdrinks.email.AvroSerdeFactory
                                        .buildEmailId(envProps),
                                andrewgrant.friendsdrinks.email.AvroSerdeFactory
                                        .buildEmail(envProps)));

        KTable<EmailId, Email> emailKTable = builder.table(emailTmpTopic,
                Consumed.with(andrewgrant.friendsdrinks.email.AvroSerdeFactory
                                .buildEmailId(envProps),
                        andrewgrant.friendsdrinks.email.AvroSerdeFactory
                                .buildEmail(envProps)));

        final String userTopic = envProps.getProperty("user.topic.name");
        KStream<UserId, UserEvent> userIdKStream = builder.stream(userTopic,
                Consumed.with(AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUser(envProps)));

        // Filter by requests.
        KStream<UserId, UserEvent> userRequestsKStream = userIdKStream.
                filter(((key, value) -> value.getEventType().equals(EventType.REQUESTED)));

        // Re-key by email for join.
        KStream<EmailId, UserEvent> userKStreamKeyedByEmail =
                userRequestsKStream.selectKey(((key, value) ->
                        new EmailId(value.getUserRequest().getEmail())));


        KStream<EmailId, Request> userAndEmail = userKStreamKeyedByEmail.leftJoin(emailKTable,
                Request::new,
                Joined.with(andrewgrant.friendsdrinks.email.AvroSerdeFactory.buildEmailId(envProps),
                        AvroSerdeFactory.buildUser(envProps),
                        andrewgrant.friendsdrinks.email.AvroSerdeFactory.buildEmail(envProps)));

        KStream<UserId, UserEvent> validatedUser =
                userAndEmail.transform(Validator::new, PENDING_EMAILS_STORE_NAME)
                        .selectKey(((key, value) -> {
                           if (value.getEventType().equals(EventType.VALIDATED)) {
                              return value.getUserValidated().getUserId();
                           } else {
                               return value.getUserRejected().getUserId();
                           }
                        }));

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        validatedUser.to(userValidationTopic,
                Produced.with(AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUser(envProps)));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        ValidationService validationService = new ValidationService();
        Properties envProps = loadEnvProperties(args[0]);
        Topology topology = validationService.buildTopology(envProps);
        log.debug("Built stream");

        Properties streamProps = validationService.buildStreamsProperties(envProps);
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
