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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.user.UserAvro;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Tests for validation.
 */
public class ValidationServiceTest {

    private static ValidationService service;
    private static Properties envProps;
    private static Topology topology;
    private static Properties streamProps;
    private static TopologyTestDriver testDriver;

    @BeforeClass
    public static void setup() throws IOException {
        service = new ValidationService();
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
    public void testValidationCreateUserRequest() {
        SpecificAvroSerializer<UserEvent> userSerializer = UserAvro.userEventSerializer(envProps);
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);

        SpecificAvroSerializer<Email> emailSerializer = EmailAvro.emailSerializer(envProps);
        SpecificAvroSerializer<EmailId> emailIdSerializer = EmailAvro.emailIdSerializer(envProps);

        List<Email> emailInput = new ArrayList<>();
        String takenEmail = "takenemail@test.com";
        String userId = UUID.randomUUID().toString();
        emailInput.add(Email.newBuilder()
                .setEmailId(new EmailId(takenEmail))
                .setUserId(userId)
                .setEventType(EmailEvent.RESERVED)
                .build());

        ConsumerRecordFactory<EmailId, Email> emailInputFactory =
                new ConsumerRecordFactory<>(emailIdSerializer, emailSerializer);

        final String emailTopic = envProps.getProperty("email.topic.name");
        for (Email email : emailInput) {
            testDriver.pipeInput(
                    emailInputFactory.create(emailTopic, email.getEmailId(), email));
        }

        List<UserEvent> userInput = new ArrayList<>();
        String newRequestId = UUID.randomUUID().toString();
        String newUserId = UUID.randomUUID().toString();
        String newEmail = UUID.randomUUID().toString();
        // Valid request.
        CreateUserRequest userRequest = CreateUserRequest.newBuilder()
                .setRequestId(newRequestId)
                .setUserId(new UserId(newUserId))
                .setEmail(newEmail)
                .build();
        userInput.add(UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(userRequest)
                .build());

        String newUserId2 = UUID.randomUUID().toString();
        // Invalid request for taken email.
        CreateUserRequest userRequest2 = CreateUserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(takenEmail)
                .setUserId(new UserId(newUserId2))
                .build();
        userInput.add(UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(userRequest2)
                .build());

        String newUserId3 = UUID.randomUUID().toString();
        CreateUserRequest userRequest3 = CreateUserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(newEmail)
                .setUserId(new UserId(newUserId3))
                .build();

        userInput.add(UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(userRequest3)
                .build());

        ConsumerRecordFactory<UserId, UserEvent> userInputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : userInput) {
            testDriver.pipeInput(
                    userInputFactory.create(userTopic,
                            user.getCreateUserRequest().getUserId(), user));
        }

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        SpecificAvroDeserializer<UserId> userIdDeserializer = UserAvro.userIdDeserializer(envProps);
        SpecificAvroDeserializer<UserEvent> userDeserializer = UserAvro.userDeserializer(envProps);

        List<UserEvent> userValidationOutput = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userRecord = testDriver.readOutput(
                    userValidationTopic, userIdDeserializer, userDeserializer);
            if (userRecord != null) {
                EventType eventType = userRecord.value().getEventType();
                if (eventType.equals(EventType.CREATE_USER_REJECTED) ||
                        eventType.equals(EventType.CREATE_USER_VALIDATED)) {
                    userValidationOutput.add(userRecord.value());
                }
            } else {
                break;
            }
        }

        assertEquals(3, userValidationOutput.size());

        UserEvent validatedUser = userValidationOutput.get(0);
        assertEquals(newUserId, validatedUser.getCreateUserValidated().getUserId().getId());
        assertEquals(EventType.CREATE_USER_VALIDATED, validatedUser.getEventType());

        UserEvent rejectedUser = userValidationOutput.get(1);
        assertEquals(newUserId2, rejectedUser.getCreateUserRejected().getUserId().getId());
        assertEquals(EventType.CREATE_USER_REJECTED, rejectedUser.getEventType());
        assertEquals(ErrorCode.EXISTS.toString(),
                rejectedUser.getCreateUserRejected().getErrorCode());

        UserEvent rejectedUser2 = userValidationOutput.get(2);
        assertEquals(newUserId3, rejectedUser2.getCreateUserRejected().getUserId().getId());
        assertEquals(EventType.CREATE_USER_REJECTED, rejectedUser.getEventType());
        assertEquals(ErrorCode.PENDING.toString(),
                rejectedUser2.getCreateUserRejected().getErrorCode());
    }

    @Test
    public void testValidationDeleteUserRequest() {
        SpecificAvroSerializer<UserEvent> userSerializer = UserAvro.userEventSerializer(envProps);
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);

        SpecificAvroSerializer<Email> emailSerializer = EmailAvro.emailSerializer(envProps);
        SpecificAvroSerializer<EmailId> emailIdSerializer = EmailAvro.emailIdSerializer(envProps);

        List<Email> emailInput = new ArrayList<>();
        String takenEmail = "takenemail@test.com";
        String userId = UUID.randomUUID().toString();
        emailInput.add(Email.newBuilder()
                .setEmailId(new EmailId(takenEmail))
                .setUserId(userId)
                .setEventType(EmailEvent.RESERVED)
                .build());

        ConsumerRecordFactory<EmailId, Email> emailInputFactory =
                new ConsumerRecordFactory<>(emailIdSerializer, emailSerializer);

        final String emailTopic = envProps.getProperty("email.topic.name");
        for (Email email : emailInput) {
            testDriver.pipeInput(
                    emailInputFactory.create(emailTopic, email.getEmailId(), email));
        }

        List<UserEvent> userInput = new ArrayList<>();
        // Valid request.
        DeleteUserRequest validRequest = DeleteUserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setUserId(new UserId(userId))
                .build();
        DeleteUserRequest invalidRequest = DeleteUserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setUserId(new UserId(UUID.randomUUID().toString()))
                .build();
        userInput.add(UserEvent.newBuilder()
                .setEventType(EventType.DELETE_USER_REQUEST)
                .setDeleteUserRequest(validRequest)
                .build());
        userInput.add(UserEvent.newBuilder()
                .setEventType(EventType.DELETE_USER_REQUEST)
                .setDeleteUserRequest(invalidRequest)
                .build());

        ConsumerRecordFactory<UserId, UserEvent> userInputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : userInput) {
            testDriver.pipeInput(
                    userInputFactory.create(userTopic,
                            user.getDeleteUserRequest().getUserId(), user));
        }

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        SpecificAvroDeserializer<UserId> userIdDeserializer = UserAvro.userIdDeserializer(envProps);
        SpecificAvroDeserializer<UserEvent> userDeserializer = UserAvro.userDeserializer(envProps);

        List<UserEvent> userValidationOutput = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userRecord = testDriver.readOutput(
                    userValidationTopic, userIdDeserializer, userDeserializer);
            if (userRecord != null) {
                EventType eventType = userRecord.value().getEventType();
                if (eventType.equals(EventType.DELETE_USER_REJECTED) ||
                        eventType.equals(EventType.DELETE_USER_VALIDATED)) {
                    userValidationOutput.add(userRecord.value());
                }
            } else {
                break;
            }
        }

        assertEquals(2, userValidationOutput.size());
        UserEvent userEvent1 = userValidationOutput.get(0);
        assertEquals(EventType.DELETE_USER_VALIDATED, userEvent1.getEventType());
        UserEvent userEvent2 = userValidationOutput.get(1);
        assertEquals(EventType.DELETE_USER_REJECTED, userEvent2.getEventType());
    }
}
