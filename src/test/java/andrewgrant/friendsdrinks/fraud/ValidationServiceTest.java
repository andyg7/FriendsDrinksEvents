package andrewgrant.friendsdrinks.fraud;

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


import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import andrewgrant.friendsdrinks.avro.UserId;
import andrewgrant.friendsdrinks.user.UserAvro;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;


/**
 * Tests fraud validation service.
 */
public class ValidationServiceTest {

    /**
     * Tests validate.
     */
    @Test
    public void testValidate() throws IOException {
        ValidationService validationService = new ValidationService();
        Properties envProps = loadEnvProperties(TEST_CONFIG_FILE);
        Topology topology = validationService.buildTopology(envProps);

        Properties streamProps = validationService.buildStreamsProperties(envProps);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamProps);

        SpecificAvroSerializer<User> userSerializer = UserAvro.userSerializer(envProps);
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);

        List<User> userInput = new ArrayList<>();
        String userId1 = UUID.randomUUID().toString();
        // Valid request.
        userInput.add(User.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setEventType(UserEvent.REQUESTED)
                .setUserId(new UserId(userId1))
                .build());

        String userId2 = UUID.randomUUID().toString();
        // Invalid request for taken email.
        userInput.add(User.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setEventType(UserEvent.REQUESTED)
                .setUserId(new UserId(userId2))
                .build());

        String userId3 = UUID.randomUUID().toString();
        userInput.add(User.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setEventType(UserEvent.REQUESTED)
                .setUserId(new UserId(userId3))
                .build());


        ConsumerRecordFactory<UserId, User> userInputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        for (User user : userInput) {
            testDriver.pipeInput(
                    userInputFactory.create(userTopic, user.getUserId(), user));
        }

        // DoS from user id 4.
        String userId4 = UUID.randomUUID().toString();
        for (int i = 0; i < 15; i++) {
            User user = User.newBuilder()
                    .setRequestId(UUID.randomUUID().toString())
                    .setEmail(UUID.randomUUID().toString())
                    .setEventType(UserEvent.REQUESTED)
                    .setUserId(new UserId(userId4))
                    .build();
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

        assertEquals(18, userValidationOutput.size());

        User validatedUser = userValidationOutput.get(0);
        assertEquals(userId1, validatedUser.getUserId().getId());
        assertEquals(UserEvent.VALIDATED, validatedUser.getEventType());
        assertEquals(null, validatedUser.getErrorCode());

        User rejectedUser = userValidationOutput.get(17);
        assertEquals(userId4, rejectedUser.getUserId().getId());
        assertEquals(UserEvent.REJECTED, rejectedUser.getEventType());
        assertEquals("DOS", rejectedUser.getErrorCode());
    }

}
