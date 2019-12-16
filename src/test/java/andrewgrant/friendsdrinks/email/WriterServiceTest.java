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
public class WriterServiceTest {

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
        final String userTopic = envProps.getProperty("user.topic.name");
        registryClient.register(userTopic + "-key", UserId.getClassSchema());
        registryClient.register(userTopic + "-value", UserEvent.getClassSchema());
        // user validation topic
        final String userValidationTopic = envProps.getProperty("user_validation.topic.name");
        registryClient.register(userValidationTopic + "-key", UserId.getClassSchema());
        registryClient.register(userValidationTopic + "-value", UserEvent.getClassSchema());
        // email topic
        final String emailTopic = envProps.getProperty("email.topic.name");
        registryClient.register(emailTopic + "-key", EmailId.getClassSchema());
        registryClient.register(emailTopic + "-value", EmailEvent.getClassSchema());
        final String emailTmp1Topic = envProps.getProperty("email_tmp_1.topic.name");
        registryClient.register(emailTmp1Topic + "-key", EmailId.getClassSchema());
        registryClient.register(emailTmp1Topic + "-value", EmailEvent.getClassSchema());
        final String emailTmp2Topic = envProps.getProperty("email_tmp_2.topic.name");
        registryClient.register(emailTmp2Topic + "-key", UserId.getClassSchema());
        registryClient.register(emailTmp2Topic + "-value", EmailEvent.getClassSchema());
        final String emailTmp3Topic = envProps.getProperty("email_tmp_3.topic.name");
        registryClient.register(emailTmp3Topic + "-key", UserId.getClassSchema());
        registryClient.register(emailTmp3Topic + "-value", EmailEvent.getClassSchema());
        userAvro = new UserAvro(envProps, registryClient);
        emailAvro = new EmailAvro(envProps, registryClient);

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
        CreateUserResponse createUserResponseSuccess = CreateUserResponse.newBuilder()
                .setRequestId("1")
                .setUserId(new UserId(UUID.randomUUID().toString()))
                .setEmail("email1")
                .setResult(Result.SUCCESS)
                .build();

        CreateUserResponse createUserResponseFail = CreateUserResponse.newBuilder()
                .setRequestId("2")
                .setUserId(new UserId(UUID.randomUUID().toString()))
                .setEmail("email2")
                .setResult(Result.FAIL)
                .build();

        input.add(
                UserEvent.newBuilder()
                        .setCreateUserResponse(createUserResponseSuccess)
                        .setEventType(EventType.CREATE_USER_RESPONSE).build());
        input.add(
                UserEvent.newBuilder()
                        .setCreateUserResponse(createUserResponseFail)
                        .setEventType(EventType.CREATE_USER_RESPONSE).build());

        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : input) {
            testDriver.pipeInput(inputFactory.create(userTopic,
                    user.getCreateUserResponse().getUserId(), user));
        }

        Deserializer<EmailId> emailIdDeserializer = emailAvro
                .emailIdDeserializer();
        Deserializer<EmailEvent> emailDeserializer = emailAvro
                .emailDeserializer();

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
        Serializer<EmailEvent> emailSerializer = emailAvro.emailSerializer();
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

        final String userTopic = envProps.getProperty("user.topic.name");
        for (UserEvent user : input) {
            testDriver.pipeInput(userInputFactory.create(userTopic,
                    user.getDeleteUserResponse().getUserId(), user));
        }

        Deserializer<EmailId> emailIdDeserializer = emailAvro
                .emailIdDeserializer();
        Deserializer<EmailEvent> emailDeserializer = emailAvro
                .emailDeserializer();

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
