package andrewgrant.friendsdrinks.user;

import static org.junit.Assert.*;

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

import andrewgrant.friendsdrinks.fraud.ErrorCode;
import andrewgrant.friendsdrinks.user.avro.*;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

/**
 * Tests validation aggregator.
 */
public class ValidationAggregatorServiceTest {

    private static Topology topology;
    private static Properties streamProps;
    private static Properties envProps;
    private static ValidationAggregatorService service;
    private static TopologyTestDriver testDriver;

    @BeforeClass
    public static void setup() throws IOException {
        service = new ValidationAggregatorService();
        envProps = loadEnvProperties(TEST_CONFIG_FILE);
        topology = service.buildTopology(envProps);
        streamProps = service.buildStreamProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamProps);
    }

    @Test
    public void testValidateValidCreateRequest() {
        String requestId = UUID.randomUUID().toString();
        String email = "hello@hello.com";
        UserId userId = UserId.newBuilder()
                .setId(UUID.randomUUID().toString())
                .build();
        CreateUserRequest userRequest = CreateUserRequest.newBuilder()
                .setRequestId(requestId)
                .setEmail(email)
                .setUserId(userId)
                .build();
        UserEvent userEventRequest = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(userRequest)
                .build();
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);
        SpecificAvroSerializer<UserEvent> userEventSerializer =
                UserAvro.userEventSerializer(envProps);

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userEventSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        // Pipe initial request to user topic.
        testDriver.pipeInput(inputFactory.create(userTopic,
                userEventRequest.getCreateUserRequest().getUserId(),
                userEventRequest));

        CreateUserValidated userValidated1 = CreateUserValidated.newBuilder()
                .setRequestId(requestId)
                .setEmail(email)
                .setUserId(userId)
                .build();

        UserEvent userEvent1 = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_VALIDATED)
                .setCreateUserValidated(userValidated1)
                .build();

        CreateUserValidated userValidated2 = CreateUserValidated.newBuilder()
                .setRequestId(requestId)
                .setEmail(email)
                .setUserId(userId)
                .build();

        UserEvent userEvent2 = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_VALIDATED)
                .setCreateUserValidated(userValidated2)
                .build();

        List<UserEvent> userEvents = new ArrayList<>();
        userEvents.add(userEvent1);
        userEvents.add(userEvent2);

        final String userValidationsTopic = envProps.getProperty("user_validation.topic.name");
        for (UserEvent userEvent : userEvents) {
            testDriver.pipeInput(inputFactory.create(userValidationsTopic,
                    userEvent.getCreateUserValidated().getUserId(),
                    userEvent));
        }

        SpecificAvroDeserializer<UserId> userIdDeserializer = UserAvro.userIdDeserializer(envProps);
        SpecificAvroDeserializer<UserEvent> userDeserializer = UserAvro.userDeserializer(envProps);
        List<UserEvent> output = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userEventRecord = testDriver.readOutput(
                    userTopic, userIdDeserializer, userDeserializer);
            if (userEventRecord != null) {
                if (userEventRecord.value().getEventType().equals(EventType.CREATE_USER_RESPONSE)) {
                    output.add(userEventRecord.value());
                }
            } else {
                break;
            }
        }

        assertEquals(1, output.size());
        assertEquals(EventType.CREATE_USER_RESPONSE, output.get(0).getEventType());
        assertEquals(Result.SUCCESS, output.get(0).getCreateUserResponse().getResult());
    }

    @Test
    public void testValidateFailedRequest() {
        String requestId = UUID.randomUUID().toString();
        String email = UUID.randomUUID().toString();
        UserId userId = UserId.newBuilder()
                .setId(UUID.randomUUID().toString())
                .build();
        CreateUserRequest userRequest = CreateUserRequest.newBuilder()
                .setRequestId(requestId)
                .setEmail(email)
                .setUserId(userId)
                .build();
        UserEvent userEventRequest = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(userRequest)
                .build();
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);
        SpecificAvroSerializer<UserEvent> userEventSerializer =
                UserAvro.userEventSerializer(envProps);

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userEventSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        // Pipe initial request to user topic.
        testDriver.pipeInput(inputFactory.create(userTopic,
                userEventRequest.getCreateUserRequest().getUserId(),
                userEventRequest));

        CreateUserValidated userValidated1 = CreateUserValidated.newBuilder()
                .setRequestId(requestId)
                .setEmail(email)
                .setUserId(userId)
                .build();

        UserEvent userEvent1 = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_VALIDATED)
                .setCreateUserValidated(userValidated1)
                .build();

        CreateUserRejected userRejected = CreateUserRejected.newBuilder()
                .setRequestId(requestId)
                .setEmail(email)
                .setUserId(userId)
                .setErrorCode(ErrorCode.DOS.toString())
                .build();

        UserEvent userEvent2 = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REJECTED)
                .setCreateUserRejected(userRejected)
                .build();

        List<UserEvent> userEvents = new ArrayList<>();
        userEvents.add(userEvent1);
        userEvents.add(userEvent2);

        final String userValidationsTopic = envProps.getProperty("user_validation.topic.name");
        for (UserEvent userEvent : userEvents) {
            if (userEvent.getEventType().equals(EventType.CREATE_USER_VALIDATED)) {
                testDriver.pipeInput(inputFactory.create(userValidationsTopic,
                        userEvent.getCreateUserValidated().getUserId(),
                        userEvent));
            } else {
                testDriver.pipeInput(inputFactory.create(userValidationsTopic,
                        userEvent.getCreateUserRejected().getUserId(),
                        userEvent));
            }
        }

        SpecificAvroDeserializer<UserId> userIdDeserializer = UserAvro.userIdDeserializer(envProps);
        SpecificAvroDeserializer<UserEvent> userDeserializer = UserAvro.userDeserializer(envProps);
        List<UserEvent> output = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userEventRecord = testDriver.readOutput(
                    userTopic, userIdDeserializer, userDeserializer);
            if (userEventRecord != null &&
                    !userEventRecord.value().getEventType().equals(EventType.CREATE_USER_REQUEST)) {
                output.add(userEventRecord.value());
            } else {
                break;
            }
        }

        assertEquals(1, output.size());
        assertEquals(EventType.CREATE_USER_RESPONSE, output.get(0).getEventType());
        assertEquals(Result.FAIL, output.get(0).getCreateUserResponse().getResult());
    }

    @Test
    public void testValidateValidDeleteRequest() {
        String requestId = UUID.randomUUID().toString();
        String email = "hello@hello.com";
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
        SpecificAvroSerializer<UserId> userIdSerializer = UserAvro.userIdSerializer(envProps);
        SpecificAvroSerializer<UserEvent> userEventSerializer =
                UserAvro.userEventSerializer(envProps);

        ConsumerRecordFactory<UserId, UserEvent> inputFactory =
                new ConsumerRecordFactory<>(userIdSerializer, userEventSerializer);
        final String userTopic = envProps.getProperty("user.topic.name");
        // Pipe initial request to user topic.
        testDriver.pipeInput(inputFactory.create(userTopic,
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

        final String userValidationsTopic = envProps.getProperty("user_validation.topic.name");
        for (UserEvent userEvent : userEvents) {
            testDriver.pipeInput(inputFactory.create(userValidationsTopic,
                    userEvent.getDeleteUserValidated().getUserId(),
                    userEvent));
        }

        SpecificAvroDeserializer<UserId> userIdDeserializer = UserAvro.userIdDeserializer(envProps);
        SpecificAvroDeserializer<UserEvent> userDeserializer = UserAvro.userDeserializer(envProps);
        List<UserEvent> output = new ArrayList<>();
        while (true) {
            ProducerRecord<UserId, UserEvent> userEventRecord = testDriver.readOutput(
                    userTopic, userIdDeserializer, userDeserializer);
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
