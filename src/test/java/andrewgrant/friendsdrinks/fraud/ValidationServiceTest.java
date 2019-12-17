package andrewgrant.friendsdrinks.fraud;

import static org.junit.Assert.*;

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

import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;


/**
 * Tests fraud validation service.
 */
public class ValidationServiceTest {

    private static Topology topology;
    private static Properties streamProps;
    private static Properties envProps;
    private static ValidationService service;
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
        // user validation topic
        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        registryClient.register(userValidationTopic + "-key", UserId.getClassSchema());
        registryClient.register(userValidationTopic + "-value", UserEvent.getClassSchema());
        // user validation topic
        final String fraudTmpTopic = envProps.getProperty("fraud_tmp.topic.name");
        registryClient.register(fraudTmpTopic + "-key", UserId.getClassSchema());
        registryClient.register(fraudTmpTopic + "-value", UserEvent.getClassSchema());
        userAvro = new UserAvro(envProps, registryClient);

        service = new ValidationService();
        topology = service.buildTopology(envProps, userAvro);
        streamProps = service.buildStreamsProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
    }

    @AfterClass
    public static void cleanup() {
        testDriver.close();
    }
    /**
     * Tests validate.
     */
    @Test
    public void testValidate() {
        Serializer<UserEvent> userEventSerializer =
                userAvro.userEventSerializer();
        Serializer<UserId> userIdSerializer =
                userAvro.userIdSerializer();

        List<UserEvent> userEvents = new ArrayList<>();
        String userId1 = UUID.randomUUID().toString();
        // Valid request.
        CreateUserRequest userRequest1 = CreateUserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setUserId(new UserId(userId1))
                .build();

        userEvents.add(UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(userRequest1)
                .build());

        String userId2 = UUID.randomUUID().toString();
        // Invalid request for taken email.
        CreateUserRequest userRequest2 = CreateUserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setUserId(new UserId(userId2))
                .build();
        userEvents.add(UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(userRequest2)
                .build());

        String userId3 = UUID.randomUUID().toString();
        CreateUserRequest userRequest3 = CreateUserRequest.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setEmail(UUID.randomUUID().toString())
                .setUserId(new UserId(userId3))
                .build();
        userEvents.add(UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(userRequest3)
                .build());


        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userEventSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent userEvent : userEvents) {
            testDriver.pipeInput(
                    inputFactory.create(userTopic,
                            userEvent.getCreateUserRequest().getUserId(), userEvent));
        }

        // DoS from user id 4.
        String userId4 = UUID.randomUUID().toString();
        for (int i = 0; i < 15; i++) {
            CreateUserRequest userRequest = CreateUserRequest.newBuilder()
                    .setRequestId(UUID.randomUUID().toString())
                    .setEmail(UUID.randomUUID().toString())
                    .setUserId(new UserId(userId4))
                    .build();
            UserEvent user = UserEvent.newBuilder()
                    .setEventType(EventType.CREATE_USER_REQUEST)
                    .setCreateUserRequest(userRequest)
                    .build();
            testDriver.pipeInput(
                    inputFactory.create(userTopic,
                            user.getCreateUserRequest().getUserId(), user));
        }

        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        Deserializer<UserId> userIdDeserializer = userAvro.userIdDeserializer();
        Deserializer<UserEvent> userDeserializer = userAvro.userEventDeserializer();

        List<UserEvent> userValidationOutput = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userEventRecord = testDriver.readOutput(
                    userValidationTopic, userIdDeserializer, userDeserializer);
            if (userEventRecord != null) {
                userValidationOutput.add(userEventRecord.value());
            } else {
                break;
            }
        }

        assertEquals(18, userValidationOutput.size());

        UserEvent validatedUser = userValidationOutput.get(0);
        assertEquals(userId1, validatedUser.getCreateUserValidated().getUserId().getId());
        assertEquals(EventType.CREATE_USER_VALIDATED, validatedUser.getEventType());

        UserEvent rejectedUser = userValidationOutput.get(17);
        assertEquals(userId4, rejectedUser.getCreateUserRejected().getUserId().getId());
        assertEquals(EventType.CREATE_USER_REJECTED, rejectedUser.getEventType());
        assertEquals("TOO_MANY_REQUESTS", rejectedUser.getCreateUserRejected().getErrorCode());
    }

}
