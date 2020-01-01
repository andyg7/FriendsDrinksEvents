package andrewgrant.friendsdrinks.user;

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

import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Tests validation aggregator.
 */
public class DeleteUserValidationAggregatorTest {

    private static Properties envProps;
    private static TopologyTestDriver testDriver;
    private static UserAvro userAvro;

    @BeforeClass
    public static void setup() throws IOException, RestClientException {
        envProps = loadEnvProperties(TEST_CONFIG_FILE);
        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
        // user topic
        final String userTopicName = envProps.getProperty("user.topic.name");
        registryClient.register(userTopicName + "-key", UserId.getClassSchema());
        registryClient.register(userTopicName + "-value", UserEvent.getClassSchema());
        // user validation topic
        final String userValidationTopic = envProps.getProperty("userValidation.topic.name");
        registryClient.register(userValidationTopic + "-key", UserId.getClassSchema());
        registryClient.register(userValidationTopic + "-value", UserEvent.getClassSchema());
        userAvro = new UserAvro(
                envProps.getProperty("schema.registry.url"),
                registryClient);

        DeleteUserValidationAggregatorService service = new DeleteUserValidationAggregatorService();
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
        String requestId = UUID.randomUUID().toString();
        UserId userId = UserId.newBuilder()
                .setId(UUID.randomUUID().toString())
                .build();
        DeleteUserRequest userRequest = DeleteUserRequest.newBuilder()
                .setRequestId(requestId)
                .setUserId(userId)
                .build();
        UserEvent userEventRequest = UserEvent.newBuilder()
                .setEventType(EventType.DELETE_USER_REQUEST)
                .setDeleteUserRequest(userRequest)
                .build();
        Serializer<UserId> userIdSerializer = userAvro.userIdSerializer();
        Serializer<UserEvent> userEventSerializer =
                userAvro.userEventSerializer();

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userEventSerializer);
        final String userTopicName = envProps.getProperty("user.topic.name");
        // Pipe initial request to user topic.
        testDriver.pipeInput(inputFactory.create(userTopicName,
                userEventRequest.getDeleteUserRequest().getUserId(),
                userEventRequest));

        DeleteUserValidated userValidated = DeleteUserValidated.newBuilder()
                .setRequestId(requestId)
                .setUserId(userId)
                .build();

        UserEvent validatedUserEvent = UserEvent.newBuilder()
                .setEventType(EventType.DELETE_USER_VALIDATED)
                .setDeleteUserValidated(userValidated)
                .build();

        List<UserEvent> userEvents = new ArrayList<>();
        userEvents.add(validatedUserEvent);
        userEvents.add(validatedUserEvent);

        final String userValidationsTopic = envProps.getProperty("userValidation.topic.name");
        for (UserEvent userEvent : userEvents) {
            testDriver.pipeInput(inputFactory.create(userValidationsTopic,
                    userEvent.getDeleteUserValidated().getUserId(),
                    userEvent));
        }

        Deserializer<UserId> userIdDeserializer = userAvro.userIdDeserializer();
        Deserializer<UserEvent> userDeserializer = userAvro.userEventDeserializer();
        List<UserEvent> output = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userEventRecord = testDriver.readOutput(
                    userTopicName, userIdDeserializer, userDeserializer);
            if (userEventRecord != null) {
                if (userEventRecord.value().getEventType().equals(EventType.DELETE_USER_RESPONSE)) {
                    output.add(userEventRecord.value());
                }
            } else {
                break;
            }
        }

        assertEquals(1, output.size());
        assertEquals(EventType.DELETE_USER_RESPONSE, output.get(0).getEventType());
        assertEquals(Result.SUCCESS, output.get(0).getDeleteUserResponse().getResult());
    }

}
