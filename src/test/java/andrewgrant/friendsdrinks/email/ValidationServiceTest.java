package andrewgrant.friendsdrinks.email;

import static org.junit.Assert.*;

import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;
import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
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

    /**
     * Integration test that requires kafka and schema registry to be running.
     * @throws IOException
     */
    @Test
    public void testValidation() throws IOException {
        ValidationService validatorService = new ValidationService();
        Properties envProps = loadEnvProperties(TEST_CONFIG_FILE);
        Topology topology = validatorService.buildTopology(envProps);

        Properties streamProps = validatorService.buildStreamsProperties(envProps);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamProps);

        SpecificAvroSerializer<User> userSerializer = UserAvro.userSerializer(envProps);
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

        List<User> userInput = new ArrayList<>();
        String newRequestId = UUID.randomUUID().toString();
        String newUserId = UUID.randomUUID().toString();
        String newEmail = UUID.randomUUID().toString();
        // Valid request.
        userInput.add(User.newBuilder()
                .setRequestId(newRequestId)
                .setEmail(newEmail)
                .setEventType(UserEvent.REQUESTED)
                .setUserId(new UserId(newUserId))
                .build());

        String newUserId2 = UUID.randomUUID().toString();
        // Invalid request for taken email.
        userInput.add(User.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(takenEmail)
                .setEventType(UserEvent.REQUESTED)
                .setUserId(new UserId(newUserId2))
                .build());

        String newUserId3 = UUID.randomUUID().toString();
        userInput.add(User.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(newEmail)
                .setEventType(UserEvent.REQUESTED)
                .setUserId(new UserId(newUserId3))
                .build());

        ConsumerRecordFactory<UserId, User> userInputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        for (User user : userInput) {
            testDriver.pipeInput(
                    userInputFactory.create(userTopic, user.getUserId(), user));
        }

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        SpecificAvroDeserializer<UserId> userIdDeserializer = UserAvro.userIdDeserializer(envProps);
        SpecificAvroDeserializer<User> userDeserializer = UserAvro.userDeserializer(envProps);

        List<User> userValidationOutput = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, User> userRecord = testDriver.readOutput(
                    userValidationTopic, userIdDeserializer, userDeserializer);
            if (userRecord != null) {
                userValidationOutput.add(userRecord.value());
            } else {
                break;
            }
        }

        assertEquals(3, userValidationOutput.size());

        User validatedUser = userValidationOutput.get(0);
        assertEquals(newUserId, validatedUser.getUserId().getId());
        assertEquals(UserEvent.VALIDATED, validatedUser.getEventType());
        assertEquals(null, validatedUser.getErrorCode());

        User rejectedUser = userValidationOutput.get(1);
        assertEquals(newUserId2, rejectedUser.getUserId().getId());
        assertEquals(UserEvent.REJECTED, rejectedUser.getEventType());
        assertEquals(ErrorCode.EXISTS.toString(), rejectedUser.getErrorCode());

        User rejectedUser2 = userValidationOutput.get(2);
        assertEquals(newUserId3, rejectedUser2.getUserId().getId());
        assertEquals(UserEvent.REJECTED, rejectedUser.getEventType());
        assertEquals(ErrorCode.PENDING.toString(), rejectedUser2.getErrorCode());
    }

}
