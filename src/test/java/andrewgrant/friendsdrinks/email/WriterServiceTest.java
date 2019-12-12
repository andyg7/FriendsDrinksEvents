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
    public void testWriteServiceCreateUserResponse() {
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);
        SpecificAvroSerializer<UserEvent> userSerializer = UserAvro.userEventSerializer(envProps);

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);

        List<UserEvent> input = new ArrayList<>();
        CreateUserResponse createUserResponseSuccess = CreateUserResponse.newBuilder()
                .setRequestId("1")
                .setUserId(new UserId(UUID.randomUUID().toString()))
                .setEmail("email1")
                .setResult(Result.SUCCESS)
                .build();

        CreateUserResponse createUserResponseFail = CreateUserResponse.newBuilder()
                .setRequestId("2")
                .setUserId(new UserId(UUID.randomUUID().toString()))
                .setEmail("email2")
                .setResult(Result.FAIL)
                .build();

        input.add(
                UserEvent.newBuilder()
                        .setCreateUserResponse(createUserResponseSuccess)
                        .setEventType(EventType.CREATE_USER_RESPONSE).build());
        input.add(
                UserEvent.newBuilder()
                        .setCreateUserResponse(createUserResponseFail)
                        .setEventType(EventType.CREATE_USER_RESPONSE).build());

        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : input) {
            testDriver.pipeInput(inputFactory.create(userTopic,
                    user.getCreateUserResponse().getUserId(), user));
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

    @Test
    public void testWriteServiceDeleteUserResponse() throws InterruptedException {
        SpecificAvroSerializer<EmailId> emailIdSerializer = EmailAvro.emailIdSerializer(envProps);
        SpecificAvroSerializer<Email> emailSerializer = EmailAvro.emailSerializer(envProps);
        ConsumerRecordFactory<EmailId, Email> emailInputFactory =
                new ConsumerRecordFactory<>(emailIdSerializer, emailSerializer);
        UserId userId = UserId.newBuilder()
                .setId("userId")
                .build();
        Email email = Email.newBuilder()
                .setEventType(EmailEvent.RESERVED)
                .setEmailId(EmailId.newBuilder().setEmailAddress("hello").build())
                .setUserId(userId.getId())
                .build();

        final String emailTopic = envProps.getProperty("email.topic.name");
        testDriver.pipeInput(emailInputFactory.create(emailTopic,
                email.getEmailId(), email));

        Thread.sleep(2000);

        List<UserEvent> input = new ArrayList<>();
        DeleteUserResponse deleteUserResponseSuccess = DeleteUserResponse.newBuilder()
                .setRequestId("1")
                .setUserId(userId)
                .setResult(Result.SUCCESS)
                .build();
        input.add(
                UserEvent.newBuilder()
                        .setDeleteUserResponse(deleteUserResponseSuccess)
                        .setEventType(EventType.DELETE_USER_RESPONSE).build());

        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);
        SpecificAvroSerializer<UserEvent> userSerializer = UserAvro.userEventSerializer(envProps);
        ConsumerRecordFactory<UserId, UserEvent> userInputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);

        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : input) {
            testDriver.pipeInput(userInputFactory.create(userTopic,
                    user.getDeleteUserResponse().getUserId(), user));
        }

        SpecificAvroDeserializer<EmailId> emailIdDeserializer = EmailAvro
                .emailIdDeserializer(envProps);
        SpecificAvroDeserializer<Email> emailDeserializer = EmailAvro
                .emailDeserializer(envProps);

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
        assertEquals(userId.getId(), email.getUserId());
    }
}
