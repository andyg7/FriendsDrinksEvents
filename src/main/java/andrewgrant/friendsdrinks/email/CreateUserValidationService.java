package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.env.Properties.load;

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

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.api.avro.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;


/**
 * Service for validating create user requests based on email.
 */
public class CreateUserValidationService {
    private static final Logger log = LoggerFactory.getLogger(CreateUserValidationService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("email_create_user_validation_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        return props;
    }

    public static final String PENDING_EMAILS_STORE_NAME = "pending_emails_state_store_name";

    public Topology buildTopology(Properties envProps,
                                  UserAvro userAvro,
                                  EmailAvro emailAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder pendingEmails = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(PENDING_EMAILS_STORE_NAME),
                        Serdes.String(), Serdes.String()).withLoggingEnabled(new HashMap<>());
        builder.addStateStore(pendingEmails);

        final String emailTopic = envProps.getProperty("email.topic.name");
        KStream<EmailId, EmailEvent> emailKStream = builder.stream(emailTopic,
                Consumed.with(emailAvro.emailIdSerde(), emailAvro.emailEventSerde()));

        // Immediately remove rejected emails from state store.
        emailKStream.filter((key, value) -> value.getEventType().equals(
                andrewgrant.friendsdrinks.email.avro.EventType.REJECTED))
                .process(PendingEmailsStateStoreCleaner::new, PENDING_EMAILS_STORE_NAME);

        // Update changelog topic that holds current set of reserved emails.
        // Topic is not private and will be consumed by other services.
        final String currEmailTopicName = envProps.getProperty("currEmail.topic.name");
        emailKStream.filter(((key, value) -> value.getEventType().equals(
                andrewgrant.friendsdrinks.email.avro.EventType.RESERVED) ||
                value.getEventType().equals(andrewgrant.friendsdrinks.email.avro.EventType.RETURNED)))
                .mapValues(value -> {
                    if (value.getEventType().equals(andrewgrant.friendsdrinks.email.avro.EventType.RESERVED)) {
                        return value;
                    } else if (value.getEventType().equals(andrewgrant.friendsdrinks.email.avro.EventType.RETURNED)) {
                        return null;
                    } else {
                        throw new RuntimeException(String.format("Did not expect event type %s",
                                value.getEventType().toString()));
                    }
                })
                .to(currEmailTopicName, Produced.with(emailAvro.emailIdSerde(), emailAvro.emailEventSerde()));

        // Rebuild table. id -> reserved emails.
        KTable<EmailId, EmailEvent> emailKTable = builder.table(currEmailTopicName, emailAvro.consumedWith());

        // Now that reserved emails are in the KTable, its safe to remove them
        // from the state store.
        emailKTable.toStream().filter(((key, value) -> value != null &&
                value.getEventType().equals(andrewgrant.friendsdrinks.email.avro.EventType.RESERVED)))
                .process(PendingEmailsStateStoreCleaner::new, PENDING_EMAILS_STORE_NAME);

        final String userTopicName = envProps.getProperty("user_api.topic.name");
        KStream<UserId, UserEvent> userIdKStream = builder.stream(userTopicName,
                userAvro.consumedWith());

        // Filter by requests so we have a stream of user requests.
        KStream<UserId, CreateUserRequest> createUserRequests = userIdKStream.
                filter(((key, value) -> value.getEventType().equals(EventType.CREATE_USER_REQUEST)))
                .mapValues(value -> value.getCreateUserRequest());

        // Re-key by email for join.
        KStream<EmailId, CreateUserRequest> createUserRequestsKeyedByEmailId =
                createUserRequests.selectKey(((key, value) ->
                        new EmailId(value.getEmail())));

        KStream<EmailId, CreateRequest> createUserAndEmail =
                createUserRequestsKeyedByEmailId.leftJoin(emailKTable,
                        CreateRequest::new, Joined.with(
                                emailAvro.emailIdSerde(),
                                userAvro.createUserRequestSerde(),
                                emailAvro.emailEventSerde()));

        KStream<UserId, UserEvent> validatedCreateUser =
                createUserAndEmail.transform(Validator::new, PENDING_EMAILS_STORE_NAME)
                        .selectKey(((key, value) -> {
                           if (value.getEventType().equals(EventType.CREATE_USER_VALIDATED)) {
                              return value.getCreateUserValidated().getUserId();
                           } else {
                               return value.getCreateUserRejected().getUserId();
                           }
                        }));

        final String userValidationTopic = envProps.getProperty("userValidation.topic.name");
        validatedCreateUser.to(userValidationTopic, userAvro.producedWith());

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        CreateUserValidationService validationService = new CreateUserValidationService();
        Properties envProps = load(args[0]);
        UserAvro userAvro = new UserAvro(envProps.getProperty("schema.registry.url"));
        EmailAvro emailAvro = new EmailAvro(envProps.getProperty("schema.registry.url"));
        Topology topology = validationService.buildTopology(envProps,
                userAvro, emailAvro);

        log.debug("Built stream");
        log.info("Topology description:\n {}", topology.describe());

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
