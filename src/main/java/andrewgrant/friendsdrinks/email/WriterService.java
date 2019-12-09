package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.user.AvroSerdeFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Service responsible for writing to the email topic.
 * See Single Writer Principle https://www.confluent.io/blog/build-services-backbone-events/
 */
public class WriterService {
    private static final Logger log = LoggerFactory.getLogger(WriterService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("email_writer_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        return props;
    }

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopic = envProps.getProperty("user.topic.name");
        KStream<UserId, UserEvent> userValidations = builder.stream(userTopic,
                Consumed.with(AvroSerdeFactory.buildUserId(envProps),
                        AvroSerdeFactory.buildUserEvent(envProps)));

        KStream<UserId, Email> emailKStream = userValidations.filter(((key, value) ->
                value.getEventType().equals(EventType.VALIDATED) ||
                        value.getEventType().equals(EventType.REJECTED) ||
                        value.getEventType().equals(EventType.DELETED)
        )).mapValues((key, value) -> {
            EmailEvent emailEvent;
            if (value.getEventType().equals(EventType.VALIDATED)) {
                Email email = new Email();
                UserValidated userValidated = value.getUserValidated();
                email.setEmailId(new EmailId(userValidated.getEmail()));
                emailEvent = EmailEvent.RESERVED;
                email.setEventType(emailEvent);
                email.setUserId(userValidated.getUserId().getId());
                return email;
            } else if (value.getEventType().equals(EventType.REJECTED)) {
                Email email = new Email();
                UserRejected userRejected = value.getUserRejected();
                email.setEmailId(new EmailId(userRejected.getEmail()));
                emailEvent = EmailEvent.REJECTED;
                email.setEventType(emailEvent);
                email.setUserId(userRejected.getUserId().getId());
                return email;
            } else if (value.getEventType().equals(EventType.DELETED)) {
                Email email = new Email();
                UserDeleted userDeleted = value.getUserDeleted();
                email.setEmailId(new EmailId(userDeleted.getEmail()));
                emailEvent = EmailEvent.RETURNED;
                email.setEventType(emailEvent);
                email.setUserId(userDeleted.getUserId().getId());
                return email;
            } else {
                throw new RuntimeException(String.format("Received unknown event type %s",
                        value.getEventType().toString()));
            }
        });

        // Re-key on email before publishing to email topic.
        KStream<EmailId, Email> emailKStreamRekeyed =
                emailKStream.selectKey(((key, value) -> value.getEmailId()));

        final String emailTopic = envProps.getProperty("email.topic.name");
        emailKStreamRekeyed.to(emailTopic,
                Produced.with(andrewgrant.friendsdrinks.email.AvroSerdeFactory
                                .buildEmailId(envProps),
                        andrewgrant.friendsdrinks.email.AvroSerdeFactory
                                .buildEmail(envProps)));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: " +
                    "the path to an environment configuration file.");
        }

        WriterService writerService = new WriterService();
        Properties envProps = loadEnvProperties(args[0]);
        Topology topology = writerService.buildTopology(envProps);
        log.debug("Built stream");

        Properties streamProps = writerService.buildStreamsProperties(envProps);
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
