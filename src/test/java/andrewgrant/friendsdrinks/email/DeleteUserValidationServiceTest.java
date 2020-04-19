package andrewgrant.friendsdrinks.email;

import static org.junit.Assert.*;

import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;
import static andrewgrant.friendsdrinks.env.Properties.load;

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

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Tests validation logic for delete user aggregator.
 */
public class DeleteUserValidationServiceTest {

    private static DeleteUserValidationService service;
    private static Properties envProps;
    private static Topology topology;
    private static Properties streamProps;
    private static TopologyTestDriver testDriver;
    private static UserAvro userAvro;
    private static EmailAvro emailAvro;

    @BeforeClass
    public static void setup() throws IOException, RestClientException {
        envProps = load(TEST_CONFIG_FILE);

        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
        // user topic
        final String userTopicName = envProps.getProperty("user_api.topic.name");
        registryClient.register(userTopicName + "-key", UserId.getClassSchema());
        registryClient.register(userTopicName + "-value", UserEvent.getClassSchema());
        // user validation topic
        final String userValidationTopic = envProps.getProperty("userValidation.topic.name");
        registryClient.register(userValidationTopic + "-key", UserId.getClassSchema());
        registryClient.register(userValidationTopic + "-value", UserEvent.getClassSchema());
        // email topic
        final String emailTopic = envProps.getProperty("email.topic.name");
        registryClient.register(emailTopic + "-key", EmailId.getClassSchema());
        registryClient.register(emailTopic + "-value", EmailEvent.getClassSchema());
        final String emailTmp1Topic = envProps.getProperty("emailTmp1.topic.name");
        registryClient.register(emailTmp1Topic + "-key", EmailId.getClassSchema());
        registryClient.register(emailTmp1Topic + "-value", EmailEvent.getClassSchema());
        final String emailTmp2Topic = envProps.getProperty("emailTmp2.topic.name");
        registryClient.register(emailTmp2Topic + "-key", UserId.getClassSchema());
        registryClient.register(emailTmp2Topic + "-value", EmailEvent.getClassSchema());
        final String emailTmp3Topic = envProps.getProperty("emailTmp3.topic.name");
        registryClient.register(emailTmp3Topic + "-key", UserId.getClassSchema());
        registryClient.register(emailTmp3Topic + "-value", EmailEvent.getClassSchema());
        userAvro = new UserAvro(
                envProps.getProperty("schema.registry.url"),
                registryClient);
        emailAvro = new EmailAvro(
                envProps.getProperty("schema.registry.url"),
                registryClient);

        service = new DeleteUserValidationService();
        topology = service.buildTopology(envProps, userAvro, emailAvro);
        streamProps = service.buildStreamsProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
    }

    @AfterClass
    public static void cleanup() {
        testDriver.close();
    }

    @Test
    public void testValidationDeleteUserRequest() {
        Serializer<UserEvent> userSerializer = userAvro.userEventSerializer();
        Serializer<UserId> userIdSerializer = userAvro.userIdSerializer();

        Serializer<EmailEvent> emailSerializer = emailAvro.emailEventSerializer();
        Serializer<EmailId> emailIdSerializer = emailAvro.emailIdSerializer();

        List<EmailEvent> emailInput = new ArrayList<>();
        String takenEmail = "takenemail@test.com";
        String userId = UUID.randomUUID().toString();
        emailInput.add(EmailEvent.newBuilder()
                .setEmailId(new EmailId(takenEmail))
                .setUserId(userId)
                .setEventType(andrewgrant.friendsdrinks.email.avro
                        .EventType.RESERVED)
                .build());

        ConsumerRecordFactory<EmailId, EmailEvent> emailInputFactory =
                new ConsumerRecordFactory<>(emailIdSerializer, emailSerializer);

        final String emailTopic = envProps.getProperty("email.topic.name");
        for (EmailEvent email : emailInput) {
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
        final String userTopic = envProps.getProperty("user_api.topic.name");
        for (UserEvent user : userInput) {
            testDriver.pipeInput(
                    userInputFactory.create(userTopic,
                            user.getDeleteUserRequest().getUserId(), user));
        }

        final String userValidationTopic = envProps.getProperty("userValidation.topic.name");
        Deserializer<UserId> userIdDeserializer = userAvro.userIdDeserializer();
        Deserializer<UserEvent> userDeserializer = userAvro.userEventDeserializer();

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
