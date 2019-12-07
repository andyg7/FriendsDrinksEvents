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


import andrewgrant.friendsdrinks.avro.*;
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

        SpecificAvroSerializer<UserEvent> userSerializer = UserAvro.userSerializer(envProps);
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);

        List<UserEvent> userInput = new ArrayList<>();
        String userId1 = UUID.randomUUID().toString();
        // Valid request.
        UserRequest userRequest1 = UserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setUserId(new UserId(userId1))
                .build();

        userInput.add(UserEvent.newBuilder()
                .setEventType(EventType.REQUESTED)
                .setUserRequest(userRequest1)
                .build());

        String userId2 = UUID.randomUUID().toString();
        // Invalid request for taken email.
        UserRequest userRequest2 = UserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setUserId(new UserId(userId2))
                .build();
        userInput.add(UserEvent.newBuilder()
                .setEventType(EventType.REQUESTED)
                .setUserRequest(userRequest2)
                .build());

        String userId3 = UUID.randomUUID().toString();
        UserRequest userRequest3 = UserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setUserId(new UserId(userId3))
                .build();
        userInput.add(UserEvent.newBuilder()
                .setEventType(EventType.REQUESTED)
                .setUserRequest(userRequest3)
                .build());


        ConsumerRecordFactory<UserId, UserEvent> userInputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : userInput) {
            testDriver.pipeInput(
                    userInputFactory.create(userTopic,
                            user.getUserRequest().getUserId(), user));
        }

        // DoS from user id 4.
        String userId4 = UUID.randomUUID().toString();
        for (int i = 0; i < 15; i++) {
            UserRequest userRequest = UserRequest.newBuilder()
                    .setRequestId(UUID.randomUUID().toString())
                    .setEmail(UUID.randomUUID().toString())
                    .setUserId(new UserId(userId4))
                    .build();
            UserEvent user = UserEvent.newBuilder()
                    .setEventType(EventType.REQUESTED)
                    .setUserRequest(userRequest)
                    .build();
            testDriver.pipeInput(
                    userInputFactory.create(userTopic,
                            user.getUserRequest().getUserId(), user));
        }

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        SpecificAvroDeserializer<UserId> userIdDeserializer = UserAvro.userIdDeserializer(envProps);
        SpecificAvroDeserializer<UserEvent> userDeserializer = UserAvro.userDeserializer(envProps);

        List<UserEvent> userValidationOutput = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userRecord = testDriver.readOutput(
                    userValidationTopic, userIdDeserializer, userDeserializer);
            if (userRecord != null) {
                userValidationOutput.add(userRecord.value());
            } else {
                break;
            }
        }

        assertEquals(18, userValidationOutput.size());

        UserEvent validatedUser = userValidationOutput.get(0);
        assertEquals(userId1, validatedUser.getUserValidated().getUserId().getId());
        assertEquals(EventType.VALIDATED, validatedUser.getEventType());

        UserEvent rejectedUser = userValidationOutput.get(17);
        assertEquals(userId4, rejectedUser.getUserRejected().getUserId().getId());
        assertEquals(EventType.REJECTED, rejectedUser.getEventType());
        assertEquals("DOS", rejectedUser.getUserRejected().getErrorCode());
    }

}
