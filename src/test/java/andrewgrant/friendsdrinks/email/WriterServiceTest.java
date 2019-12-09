package andrewgrant.friendsdrinks.email;

import static org.junit.Assert.*;

import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;
import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.user.UserAvro;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Tests for EmailWriterService.
 */
public class WriterServiceTest {

    private static WriterService service;
    private static Properties envProps;
    private static Topology topology;
    private static Properties streamProps;
    private static TopologyTestDriver testDriver;

    @BeforeClass
    public static void setup() throws IOException {
        service = new WriterService();
        envProps = loadEnvProperties(TEST_CONFIG_FILE);
        topology = service.buildTopology(envProps);
        streamProps = service.buildStreamsProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
    }


    /**
     * Integration test that requires kafka and schema registry to be running.
     * @throws IOException
     */
    @Test
    public void testWriteService() {
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);
        SpecificAvroSerializer<UserEvent> userSerializer = UserAvro.userEventSerializer(envProps);

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);

        List<UserEvent> input = new ArrayList<>();
        UserValidated userValidated = UserValidated.newBuilder()
                .setRequestId("1")
                .setUserId(new UserId(UUID.randomUUID().toString()))
                .setEmail(UUID.randomUUID().toString())
                .build();

        UserRejected userRejected = UserRejected.newBuilder()
                .setRequestId("2")
                .setUserId(new UserId(UUID.randomUUID().toString()))
                .setEmail(UUID.randomUUID().toString())
                .setErrorCode(ErrorCode.EXISTS.toString())
                .build();

        input.add(
                UserEvent.newBuilder()
                        .setUserValidated(userValidated)
                        .setEventType(EventType.VALIDATED).build());
        input.add(
                UserEvent.newBuilder()
                        .setUserRejected(userRejected)
                        .setEventType(EventType.REJECTED).build());

        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : input) {
            if (user.getEventType().equals(EventType.VALIDATED)) {
                testDriver.pipeInput(inputFactory.create(userTopic,
                        user.getUserValidated().getUserId(), user));
            } else {
                testDriver.pipeInput(inputFactory.create(userTopic,
                        user.getUserRejected().getUserId(), user));
            }
        }

        SpecificAvroDeserializer<EmailId> emailIdDeserializer = EmailAvro
                .emailIdDeserializer(envProps);
        SpecificAvroDeserializer<Email> emailDeserializer = EmailAvro
                .emailDeserializer(envProps);

        final String emailTopic = envProps.getProperty("email.topic.name");
        List<Email> output = new ArrayList<>();
        while (true) {
            ProducerRecord<EmailId, Email> emailRecord =
                    testDriver.readOutput(emailTopic, emailIdDeserializer, emailDeserializer);
            if (emailRecord != null) {
                output.add(emailRecord.value());
            } else {
                break;
            }
        }

        assertEquals(2, output.size());
        Email email1 = output.get(0);
        assertEquals(EmailEvent.RESERVED, email1.getEventType());
        Email email2 = output.get(1);
        assertEquals(EmailEvent.REJECTED, email2.getEventType());
    }

    /**
     * Integration test that requires kafka and schema registry to be running.
     * @throws IOException
     */
    @Test
    public void testWriterServiceHandlesDeletedUser() {
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);
        SpecificAvroSerializer<UserEvent> userSerializer = UserAvro.userEventSerializer(envProps);

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);

        List<UserEvent> input = new ArrayList<>();
        UserDeleted userDeleted = UserDeleted.newBuilder()
                .setEmail("email@email.com")
                .setRequestId("1")
                .setUserId(UserId.newBuilder().setId("1").build())
                .build();

        input.add(
                UserEvent.newBuilder()
                        .setUserDeleted(userDeleted)
                        .setEventType(EventType.DELETED).build());

        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : input) {
            testDriver.pipeInput(inputFactory.create(userTopic,
                    user.getUserDeleted().getUserId(), user));
        }

        SpecificAvroDeserializer<EmailId> emailIdDeserializer = EmailAvro
                .emailIdDeserializer(envProps);
        SpecificAvroDeserializer<Email> emailDeserializer = EmailAvro
                .emailDeserializer(envProps);

        final String emailTopic = envProps.getProperty("email.topic.name");
        List<Email> output = new ArrayList<>();
        while (true) {
            ProducerRecord<EmailId, Email> emailRecord =
                    testDriver.readOutput(emailTopic, emailIdDeserializer, emailDeserializer);
            if (emailRecord != null) {
                output.add(emailRecord.value());
            } else {
                break;
            }
        }

        assertEquals(1, output.size());
        Email email1 = output.get(0);
        assertEquals(EmailEvent.RETURNED, email1.getEventType());
    }
}
