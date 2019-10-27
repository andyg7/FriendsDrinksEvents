package andrewgrant.friendsdrinks.email;

import static org.junit.Assert.*;

import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
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
public class UserEmailValidatorServiceTest {

    /**
     * Integration test that requires kafka and schema registry to be running and so is ignored
     * by default.
     * @throws IOException
     */
    @Test
    public void testValidation() throws IOException {
        UserEmailValidatorService validatorService = new UserEmailValidatorService();
        Properties envProps = validatorService.loadEnvProperties(TEST_CONFIG_FILE);
        Topology topology = validatorService.buildTopology(envProps);

        Properties streamProps = validatorService.buildStreamsProperties(envProps);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamProps);

        SpecificAvroSerializer<User> userSerializer = UserAvro.serializer(envProps);
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
                .setUserId(newUserId)
                .build());

        String newUserId2 = UUID.randomUUID().toString();
        // Invalid request for taken email.
        userInput.add(User.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(takenEmail)
                .setEventType(UserEvent.REQUESTED)
                .setUserId(newUserId2)
                .build());

        String newUserId3 = UUID.randomUUID().toString();
        userInput.add(User.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(newEmail)
                .setEventType(UserEvent.REQUESTED)
                .setUserId(newUserId3)
                .build());

        ConsumerRecordFactory<EmailId, User> userInputFactory =
                new ConsumerRecordFactory<>(emailIdSerializer, userSerializer);
        final String emailRequestTopic = envProps.getProperty("email_request.topic.name");
        for (User user : userInput) {
            testDriver.pipeInput(
                    userInputFactory.create(emailRequestTopic, new EmailId(user.getEmail()), user));
        }

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        SpecificAvroDeserializer<User> userDeserializer = UserAvro.deserializer(envProps);

        List<User> userValidationOutput = new ArrayList<>();
        while (true) {
            ProducerRecord<String, User> userRecord = testDriver.readOutput(
                    userValidationTopic, stringDeserializer, userDeserializer);
            if (userRecord != null) {
                userValidationOutput.add(userRecord.value());
            } else {
                break;
            }
        }

        assertEquals(3, userValidationOutput.size());

        User validatedUser = userValidationOutput.get(0);
        assertEquals(newUserId, validatedUser.getUserId());
        assertEquals(UserEvent.VALIDATED, validatedUser.getEventType());
        assertEquals(null, validatedUser.getErrorCode());

        User rejectedUser = userValidationOutput.get(1);
        assertEquals(newUserId2, rejectedUser.getUserId());
        assertEquals(UserEvent.REJECTED, rejectedUser.getEventType());
        assertEquals(ErrorCode.EXISTS.toString(), rejectedUser.getErrorCode());

        User rejectedUser2 = userValidationOutput.get(2);
        assertEquals(newUserId3, rejectedUser2.getUserId());
        assertEquals(UserEvent.REJECTED, rejectedUser.getEventType());
        assertEquals(ErrorCode.PENDING.toString(), rejectedUser2.getErrorCode());
    }

}
