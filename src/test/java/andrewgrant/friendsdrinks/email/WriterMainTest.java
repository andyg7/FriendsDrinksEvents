package andrewgrant.friendsdrinks.email;

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
import java.util.*;

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;


/**
 * Tests for EmailWriterService.
 */
public class WriterMainTest {

    private static WriterService service;
    private static Properties envProps;
    private static Topology topology;
    private static Properties streamProps;
    private static TopologyTestDriver testDriver;
    private static UserAvro userAvro;
    private static EmailAvro emailAvro;

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
        // email topic
        final String emailTopic = envProps.getProperty("email.topic.name");
        registryClient.register(emailTopic + "-key", EmailId.getClassSchema());
        registryClient.register(emailTopic + "-value", EmailEvent.getClassSchema());
        final String emailPrivate1Topic = envProps.getProperty("emailPrivate1.topic.name");
        registryClient.register(emailPrivate1Topic + "-key", EmailId.getClassSchema());
        registryClient.register(emailPrivate1Topic + "-value", EmailEvent.getClassSchema());
        final String emailPrivate2Topic = envProps.getProperty("emailPrivate2.topic.name");
        registryClient.register(emailPrivate2Topic + "-key", UserId.getClassSchema());
        registryClient.register(emailPrivate2Topic + "-value", EmailEvent.getClassSchema());
        final String emailPrivate3Topic = envProps.getProperty("emailPrivate3.topic.name");
        registryClient.register(emailPrivate3Topic + "-key", UserId.getClassSchema());
        registryClient.register(emailPrivate3Topic + "-value", EmailEvent.getClassSchema());
        userAvro = new UserAvro(
                envProps.getProperty("schema.registry.url"),
                registryClient);
        emailAvro = new EmailAvro(
                envProps.getProperty("schema.registry.url"),
                registryClient);

        service = new WriterService();
        topology = service.buildTopology(envProps, userAvro, emailAvro);
        streamProps = service.buildStreamsProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
    }

    @AfterClass
    public static void cleanup() {
        testDriver.close();
    }

    /**
     * Integration test that requires kafka and schema registry to be running.
     * @throws IOException
     */
    @Test
    public void testWriteServiceCreateUserResponse() {
        Serializer<UserId> userIdSerializer = userAvro.userIdSerializer();
        Serializer<UserEvent> userSerializer = userAvro.userEventSerializer();

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);

        List<UserEvent> input = new ArrayList<>();
        CreateUserRequest createUserRequestSuccess = CreateUserRequest.newBuilder()
                .setRequestId("1")
                .setUserId(new UserId("user1"))
                .setEmail("email1")
                .build();

        CreateUserResponse createUserResponseSuccess = CreateUserResponse.newBuilder()
                .setRequestId("1")
                .setUserId(new UserId("user1"))
                .setResult(Result.SUCCESS)
                .build();

        CreateUserRequest createUserRequestFail = CreateUserRequest.newBuilder()
                .setRequestId("2")
                .setUserId(new UserId("user2"))
                .setEmail("email1")
                .build();
        CreateUserResponse createUserResponseFail = CreateUserResponse.newBuilder()
                .setRequestId("2")
                .setUserId(new UserId("user2"))
                .setResult(Result.FAIL)
                .build();

        input.add(
                UserEvent.newBuilder()
                        .setCreateUserRequest(createUserRequestSuccess)
                        .setEventType(EventType.CREATE_USER_REQUEST).build());
        input.add(
                UserEvent.newBuilder()
                        .setCreateUserResponse(createUserResponseSuccess)
                        .setEventType(EventType.CREATE_USER_RESPONSE).build());
        input.add(
                UserEvent.newBuilder()
                        .setCreateUserRequest(createUserRequestFail)
                        .setEventType(EventType.CREATE_USER_REQUEST).build());
        input.add(
                UserEvent.newBuilder()
                        .setCreateUserResponse(createUserResponseFail)
                        .setEventType(EventType.CREATE_USER_RESPONSE).build());

        final String userTopicName = envProps.getProperty("user.topic.name");
        for (UserEvent user : input) {
            if (user.getCreateUserResponse() != null) {
                testDriver.pipeInput(inputFactory.create(userTopicName,
                        user.getCreateUserResponse().getUserId(), user));
            } else {
                testDriver.pipeInput(inputFactory.create(userTopicName,
                        user.getCreateUserRequest().getUserId(), user));
            }
        }

        Deserializer<EmailId> emailIdDeserializer = emailAvro
                .emailIdDeserializer();
        Deserializer<EmailEvent> emailDeserializer = emailAvro
                .emailEventDeserializer();

        final String emailTopic = envProps.getProperty("email.topic.name");
        List<EmailEvent> output = new ArrayList<>();
        while (true) {
            ProducerRecord<EmailId, EmailEvent> emailRecord =
                    testDriver.readOutput(emailTopic, emailIdDeserializer, emailDeserializer);
            if (emailRecord != null) {
                output.add(emailRecord.value());
            } else {
                break;
            }
        }

        assertEquals(2, output.size());
        EmailEvent email1 = output.get(0);
        assertEquals(andrewgrant.friendsdrinks.email.avro
                .EventType.RESERVED, email1.getEventType());
        EmailEvent email2 = output.get(1);
        assertEquals(andrewgrant.friendsdrinks.email.avro
                .EventType.REJECTED, email2.getEventType());
    }

    @Test
    public void testWriteServiceDeleteUserResponse() {
        Serializer<EmailId> emailIdSerializer = emailAvro.emailIdSerializer();
        Serializer<EmailEvent> emailSerializer = emailAvro.emailEventSerializer();
        ConsumerRecordFactory<EmailId, EmailEvent> emailInputFactory =
                new ConsumerRecordFactory<>(emailIdSerializer, emailSerializer);
        UserId userId = UserId.newBuilder()
                .setId("userId")
                .build();
        EmailEvent email = EmailEvent.newBuilder()
                .setEventType(andrewgrant.friendsdrinks.email.avro
                        .EventType.RESERVED)
                .setEmailId(EmailId.newBuilder().setEmailAddress("hello").build())
                .setUserId(userId.getId())
                .build();

        final String emailTopic = envProps.getProperty("email.topic.name");
        testDriver.pipeInput(emailInputFactory.create(emailTopic,
                email.getEmailId(), email));

        List<UserEvent> input = new ArrayList<>();
        DeleteUserResponse deleteUserResponseSuccess = DeleteUserResponse.newBuilder()
                .setRequestId("1")
                .setUserId(userId)
                .setResult(Result.SUCCESS)
                .build();
        input.add(
                UserEvent.newBuilder()
                        .setDeleteUserResponse(deleteUserResponseSuccess)
                        .setEventType(EventType.DELETE_USER_RESPONSE).build());

        Serializer<UserId> userIdSerializer = userAvro.userIdSerializer();
        Serializer<UserEvent> userSerializer = userAvro.userEventSerializer();
        ConsumerRecordFactory<UserId, UserEvent> userInputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userSerializer);

        final String userTopicName = envProps.getProperty("user.topic.name");
        for (UserEvent user : input) {
            testDriver.pipeInput(userInputFactory.create(userTopicName,
                    user.getDeleteUserResponse().getUserId(), user));
        }

        Deserializer<EmailId> emailIdDeserializer = emailAvro
                .emailIdDeserializer();
        Deserializer<EmailEvent> emailDeserializer = emailAvro
                .emailEventDeserializer();

        List<EmailEvent> output = new ArrayList<>();
        while (true) {
            ProducerRecord<EmailId, EmailEvent> emailRecord =
                    testDriver.readOutput(emailTopic, emailIdDeserializer, emailDeserializer);
            if (emailRecord != null) {
                output.add(emailRecord.value());
            } else {
                break;
            }
        }

        assertEquals(1, output.size());
        EmailEvent email1 = output.get(0);
        assertEquals(andrewgrant.friendsdrinks.email.avro
                .EventType.RETURNED, email1.getEventType());
        assertEquals(userId.getId(), email.getUserId());
    }
}
