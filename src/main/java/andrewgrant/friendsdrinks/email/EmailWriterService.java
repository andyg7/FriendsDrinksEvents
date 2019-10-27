package andrewgrant.friendsdrinks.email;

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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.user.UserAvroSerdeFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;


/**
 * Service responsible for writing to the email topic.
 * See Single Writer Principle https://www.confluent.io/blog/build-services-backbone-events/
 */
public class EmailWriterService {
    private static final Logger log = LoggerFactory.getLogger(EmailWriterService.class);

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envProps.getProperty("bootstrap.servers"));
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        return props;
    }

    private static boolean test = true;
    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopic = envProps.getProperty("user.topic.name");
        KStream<UserId, User> userValidations = builder.stream(userTopic,
                Consumed.with(UserAvroSerdeFactory.buildUserId(envProps),
                        UserAvroSerdeFactory.buildUser(envProps)));

        KStream<UserId, Email> emailKStream = userValidations.filter(((key, value) ->
                value.getEventType().equals(UserEvent.VALIDATED) ||
                        value.getEventType().equals(UserEvent.REJECTED)
        )).mapValues((key, value) -> {
            EmailEvent emailEvent;
            if (value.getEventType().equals(UserEvent.VALIDATED)) {
                emailEvent = EmailEvent.RESERVED;
            } else {
                emailEvent = EmailEvent.REJECTED;
            }
            Email email = new Email();
            email.setEmailId(new EmailId(value.getEmail()));
            email.setEventType(emailEvent);
            email.setUserId(value.getUserId().getId());
            return email;
        });


        // Re-key on email before publishing to email topic.
        KStream<EmailId, Email> emailKStreamRekeyed =
                emailKStream.selectKey(((key, value) -> value.getEmailId()));

        final String emailTopic = envProps.getProperty("email.topic.name");
        emailKStreamRekeyed.to(emailTopic,
                Produced.with(EmailAvroSerdeFactory.buildEmailId(envProps),
                        EmailAvroSerdeFactory.buildEmail(envProps)));

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

        EmailWriterService emailWriterService = new EmailWriterService();
        Properties envProps = emailWriterService.loadEnvProperties(args[0]);
        Topology topology = emailWriterService.buildTopology(envProps);
        log.debug("Built stream");

        Properties streamProps = emailWriterService.buildStreamsProperties(envProps);
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
