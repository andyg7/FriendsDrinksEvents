package andrewgrant.friendsdrinks.user;

import static org.junit.Assert.assertEquals;

import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;
import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Tests validation service.
 */
public class ValidationServiceTest {

    private static Properties envProps;
    private static TopologyTestDriver testDriver;
    private static UserAvro userAvro;

    @BeforeClass
    public static void setup() throws IOException, RestClientException {
        envProps = loadEnvProperties(TEST_CONFIG_FILE);
        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
        // user topic
        final String userTopic = envProps.getProperty("user.topic.name");
        registryClient.register(userTopic + "-key", UserId.getClassSchema());
        registryClient.register(userTopic + "-value", UserEvent.getClassSchema());
        final String userTmpTopic = envProps.getProperty("user_tmp.topic.name");
        registryClient.register(userTmpTopic + "-key", UserId.getClassSchema());
        registryClient.register(userTmpTopic + "-value", UserEvent.getClassSchema());
        // user validation topic
        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        registryClient.register(userValidationTopic + "-key", UserId.getClassSchema());
        registryClient.register(userValidationTopic + "-value", UserEvent.getClassSchema());
        userAvro = new UserAvro(
                envProps.getProperty("schema.registry.url"),
                registryClient);

        ValidationService service = new ValidationService();
        Topology topology = service.buildTopology(envProps, userAvro);
        Properties streamProps = service.buildStreamProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
    }

    @AfterClass
    public static void cleanup() {
        testDriver.close();
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

        Serializer<UserId> userIdSerializer = userAvro.userIdSerializer();
        Serializer<UserEvent> userEventSerializer =
                userAvro.userEventSerializer();

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
        Deserializer<UserId> userIdDeserializer = userAvro.userIdDeserializer();
        Deserializer<UserEvent> userDeserializer = userAvro.userEventDeserializer();
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
