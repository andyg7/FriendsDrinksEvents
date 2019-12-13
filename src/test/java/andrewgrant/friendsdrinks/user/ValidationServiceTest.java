package andrewgrant.friendsdrinks.user;

import static org.junit.Assert.assertEquals;

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

import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Tests validation service.
 */
public class ValidationServiceTest {

    private static Topology topology;
    private static Properties streamProps;
    private static Properties envProps;
    private static ValidationService service;
    private static TopologyTestDriver testDriver;

    @BeforeClass
    public static void setup() throws IOException {
        service = new ValidationService();
        envProps = loadEnvProperties(TEST_CONFIG_FILE);
        topology = service.buildTopology(envProps);
        streamProps = service.buildStreamProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
    }

    @Test
    public void testValidateValidDeleteRequest() {
        UserId userId = UserId.newBuilder()
                .setId("userId")
                .build();
        CreateUserResponse createUserResponse = CreateUserResponse
                .newBuilder()
                .setUserId(userId)
                .setResult(Result.SUCCESS)
                .setRequestId("1")
                .setEmail("email")
                .build();

        UserEvent userEventResponse = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_RESPONSE)
                .setCreateUserResponse(createUserResponse)
                .build();

        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);
        SpecificAvroSerializer<UserEvent> userEventSerializer =
                UserAvro.userEventSerializer(envProps);

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userEventSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        // Pipe initial request to user topic.
        testDriver.pipeInput(inputFactory.create(userTopic,
                userEventResponse.getCreateUserResponse().getUserId(),
                userEventResponse));

        DeleteUserRequest validDeleteRequest = DeleteUserRequest
                .newBuilder()
                .setUserId(userId)
                .setRequestId("2")
                .build();

        UserEvent userEventRequest1 = UserEvent
                .newBuilder()
                .setEventType(EventType.DELETE_USER_REQUEST)
                .setDeleteUserRequest(validDeleteRequest)
                .build();

        DeleteUserRequest invalidDeleteRequest = DeleteUserRequest
                .newBuilder()
                .setUserId(UserId.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .build())
                .setRequestId("2")
                .build();

        UserEvent userEventRequest2 = UserEvent
                .newBuilder()
                .setEventType(EventType.DELETE_USER_REQUEST)
                .setDeleteUserRequest(invalidDeleteRequest)
                .build();

        List<UserEvent> userEvents = new ArrayList<>();
        userEvents.add(userEventRequest1);
        userEvents.add(userEventRequest2);

        for (UserEvent userEvent : userEvents) {
            testDriver.pipeInput(inputFactory.create(userTopic,
                    userEvent.getDeleteUserRequest().getUserId(),
                    userEvent));
        }

        final String userValidationsTopic = envProps.getProperty("user_validation.topic.name");
        SpecificAvroDeserializer<UserId> userIdDeserializer = UserAvro.userIdDeserializer(envProps);
        SpecificAvroDeserializer<UserEvent> userDeserializer = UserAvro.userDeserializer(envProps);
        List<UserEvent> output = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userEventRecord = testDriver.readOutput(
                    userValidationsTopic, userIdDeserializer, userDeserializer);
            if (userEventRecord != null) {
                output.add(userEventRecord.value());
            } else {
                break;
            }
        }
        assertEquals(2, output.size());
        UserEvent userEventOutput1 = output.get(0);
        assertEquals(EventType.DELETE_USER_VALIDATED, userEventOutput1.getEventType());

        UserEvent userEventOutput2 = output.get(1);
        assertEquals(EventType.DELETE_USER_REJECTED, userEventOutput2.getEventType());
    }
}
