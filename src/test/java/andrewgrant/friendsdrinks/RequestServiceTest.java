package andrewgrant.friendsdrinks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;
import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import andrewgrant.friendsdrinks.avro.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Tests FriendsDrinks service.
 */
public class RequestServiceTest {

    private static Properties envProps;
    private static String friendsDrinksApiTopicName;
    private static TopologyTestDriver testDriver;
    private static FriendsDrinksAvro friendsDrinksAvro;

    @BeforeClass
    public static void setup() throws IOException, RestClientException {
        envProps = load(TEST_CONFIG_FILE);
        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
        friendsDrinksApiTopicName = envProps.getProperty("friendsdrinks_api.topic.name");
        registryClient.register(friendsDrinksApiTopicName + "-key", FriendsDrinksId.getClassSchema());
        registryClient.register(friendsDrinksApiTopicName + "-value", FriendsDrinksApi.getClassSchema());
        friendsDrinksAvro = new FriendsDrinksAvro(envProps.getProperty("schema.registry.url"), registryClient);

        RequestService service = new RequestService();
        Topology topology = service.buildTopology(envProps, friendsDrinksAvro);
        Properties streamsProps = service.buildStreamProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamsProps);
    }

    @Test
    public void testNewFriendsDrinksSuccess() {
        CreateFriendsDrinksRequest request = CreateFriendsDrinksRequest.newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId("friendsdrinks").build())
                .setRequestId("reqId")
                .setUserIds(Arrays.asList("b", "c"))
                .setAdminUserId("a")
                .setScheduleType(ScheduleType.OnDemand)
                .build();
        FriendsDrinksApi requestEvent = FriendsDrinksApi.newBuilder()
                .setApiType(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(request)
                .build();

        ConsumerRecordFactory<FriendsDrinksId, FriendsDrinksApi> inputFactory =
                new ConsumerRecordFactory<>(friendsDrinksAvro.friendsDrinksIdSerializer(), friendsDrinksAvro.friendsDrinksApiSerializer());

        testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName,
                new FriendsDrinksId(requestEvent.getCreateFriendsDrinksRequest().getAdminUserId()), requestEvent));

        Deserializer<FriendsDrinksId> friendsDrinksIdDeserializer = friendsDrinksAvro.friendsDrinksIdDeserializer();
        Deserializer<FriendsDrinksApi> friendsDrinksEventDeserializer = friendsDrinksAvro.friendsDrinksApiDeserializer();
        ProducerRecord<FriendsDrinksId, FriendsDrinksApi> output =
                testDriver.readOutput(friendsDrinksApiTopicName, friendsDrinksIdDeserializer, friendsDrinksEventDeserializer);
        assertEquals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE, output.value().getApiType());
        assertEquals(Result.SUCCESS, output.value().getCreateFriendsDrinksResponse().getResult());
    }

    @Test
    public void testNewFriendsDrinksFail() {
        String requesterUserId = UUID.randomUUID().toString();
        String requestId = "request123";
        CreateFriendsDrinksRequest request = CreateFriendsDrinksRequest.newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId("friendsdrinks").build())
                .setRequestId(requestId)
                .setUserIds(Arrays.asList("b", "c"))
                .setAdminUserId(requesterUserId)
                .setScheduleType(ScheduleType.OnDemand)
                .build();
        FriendsDrinksApi requestEvent = FriendsDrinksApi.newBuilder()
                .setApiType(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(request)
                .build();

        ConsumerRecordFactory<FriendsDrinksId, FriendsDrinksApi> inputFactory =
                new ConsumerRecordFactory<>(friendsDrinksAvro.friendsDrinksIdSerializer(), friendsDrinksAvro.friendsDrinksApiSerializer());

        for (int i = 0; i < 5; i++) {
            CreateFriendsDrinksResponse response = CreateFriendsDrinksResponse.newBuilder()
                    .setRequestId(String.valueOf(i))
                    .setResult(Result.SUCCESS)
                    .build();
            FriendsDrinksApi event = FriendsDrinksApi.newBuilder()
                    .setApiType(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE)
                    .setCreateFriendsDrinksResponse(response)
                    .build();
            testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName,
                    new FriendsDrinksId(requesterUserId), event));
        }


        testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName,
                new FriendsDrinksId(requestEvent.getCreateFriendsDrinksRequest().getAdminUserId()), requestEvent));

        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Deserializer<FriendsDrinksApi> friendsDrinksEventDeserializer = friendsDrinksAvro.friendsDrinksApiDeserializer();
        FriendsDrinksApi output = null;
        while (true) {
            ProducerRecord<String, FriendsDrinksApi> event =
                    testDriver.readOutput(friendsDrinksApiTopicName, stringDeserializer, friendsDrinksEventDeserializer);
            if (event != null) {
                if (event.value().getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                        event.value().getCreateFriendsDrinksResponse().getRequestId().equals(requestId)) {
                    output = event.value();
                }
            } else {
                break;
            }
        }
        assertEquals(Result.FAIL, output.getCreateFriendsDrinksResponse().getResult());
    }

    @Test
    public void testNewFriendsDrinksSuccessWithSomeAlreadyCreatedFriendsDrinks() {
        String requesterUserId = UUID.randomUUID().toString();
        String requestId = "request123";
        CreateFriendsDrinksRequest request = CreateFriendsDrinksRequest.newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId("friendsdrinks").build())
                .setRequestId(requestId)
                .setUserIds(Arrays.asList("b", "c"))
                .setAdminUserId(requesterUserId)
                .setScheduleType(ScheduleType.OnDemand)
                .build();
        FriendsDrinksApi requestEvent = FriendsDrinksApi.newBuilder()
                .setApiType(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(request)
                .build();

        ConsumerRecordFactory<FriendsDrinksId, FriendsDrinksApi> inputFactory =
                new ConsumerRecordFactory<>(friendsDrinksAvro.friendsDrinksIdSerializer(), friendsDrinksAvro.friendsDrinksApiSerializer());

        for (int i = 0; i < 4; i++) {
            CreateFriendsDrinksResponse response = CreateFriendsDrinksResponse.newBuilder()
                    .setRequestId(String.valueOf(i))
                    .setResult(Result.SUCCESS)
                    .build();
            FriendsDrinksApi event = FriendsDrinksApi.newBuilder()
                    .setApiType(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE)
                    .setCreateFriendsDrinksResponse(response)
                    .build();
            testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName, new FriendsDrinksId(requesterUserId), event));
        }


        testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName,
                new FriendsDrinksId(requestEvent.getCreateFriendsDrinksRequest().getAdminUserId()), requestEvent));

        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Deserializer<FriendsDrinksApi> friendsDrinksEventDeserializer = friendsDrinksAvro.friendsDrinksApiDeserializer();
        FriendsDrinksApi output = null;
        while (true) {
            ProducerRecord<String, FriendsDrinksApi> event =
                    testDriver.readOutput(friendsDrinksApiTopicName, stringDeserializer, friendsDrinksEventDeserializer);
            if (event != null) {
                if (event.value().getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                        event.value().getCreateFriendsDrinksResponse().getRequestId().equals(requestId)) {
                    output = event.value();
                }
            } else {
                break;
            }
        }
        assertEquals(Result.SUCCESS, output.getCreateFriendsDrinksResponse().getResult());
    }

    @Test
    public void testNewFriendsDrinksSuccessAfterDeletion() {
        String requesterUserId = UUID.randomUUID().toString();
        String requestId = UUID.randomUUID().toString();
        CreateFriendsDrinksRequest request = CreateFriendsDrinksRequest.newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId("friendsdrinks").build())
                .setRequestId(requestId)
                .setUserIds(Arrays.asList("b", "c"))
                .setAdminUserId(requesterUserId)
                .setScheduleType(ScheduleType.OnDemand)
                .build();
        FriendsDrinksApi requestEvent = FriendsDrinksApi.newBuilder()
                .setApiType(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(request)
                .build();

        ConsumerRecordFactory<FriendsDrinksId, FriendsDrinksApi> inputFactory =
                new ConsumerRecordFactory<>(friendsDrinksAvro.friendsDrinksIdSerializer(), friendsDrinksAvro.friendsDrinksApiSerializer());


        for (int i = 0; i < 5; i++) {
            CreateFriendsDrinksResponse response = CreateFriendsDrinksResponse.newBuilder()
                    .setRequestId(String.valueOf(i))
                    .setResult(Result.SUCCESS)
                    .build();
            FriendsDrinksApi event = FriendsDrinksApi.newBuilder()
                    .setApiType(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE)
                    .setCreateFriendsDrinksResponse(response)
                    .build();
            testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName,
                    new FriendsDrinksId(requesterUserId), event));
        }

        testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName,
                new FriendsDrinksId(requestEvent.getCreateFriendsDrinksRequest().getAdminUserId()), requestEvent));

        Deserializer<FriendsDrinksId> friendsDrinksIdDeserializer = friendsDrinksAvro.friendsDrinksIdDeserializer();
        Deserializer<FriendsDrinksApi> friendsDrinksEventDeserializer = friendsDrinksAvro.friendsDrinksApiDeserializer();
        FriendsDrinksApi output = null;
        while (true) {
            ProducerRecord<FriendsDrinksId, FriendsDrinksApi> event =
                    testDriver.readOutput(friendsDrinksApiTopicName, friendsDrinksIdDeserializer, friendsDrinksEventDeserializer);
            if (event != null) {
                if (event.value().getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                        event.value().getCreateFriendsDrinksResponse().getRequestId().equals(requestId)) {
                    output = event.value();
                }
            } else {
                break;
            }
        }
        // Test we fail first.
        assertEquals(Result.FAIL, output.getCreateFriendsDrinksResponse().getResult());

        DeleteFriendsDrinksResponse deleteFriendsDrinksResponse = DeleteFriendsDrinksResponse.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setResult(Result.SUCCESS)
                .build();
        FriendsDrinksApi deleteFriendsDrinksApi = FriendsDrinksApi.newBuilder()
                .setApiType(ApiType.DELETE_FRIENDS_DRINKS_RESPONSE)
                .setDeleteFriendsDrinksResponse(deleteFriendsDrinksResponse)
                .build();
        testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName,
                new FriendsDrinksId(requesterUserId),
                deleteFriendsDrinksApi));

        // New request id.
        requestId = UUID.randomUUID().toString();
        request = CreateFriendsDrinksRequest.newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId("friendsdrinks").build())
                .setRequestId(requestId)
                .setUserIds(Arrays.asList("b", "c"))
                .setAdminUserId(requesterUserId)
                .setScheduleType(ScheduleType.OnDemand)
                .build();
        requestEvent = FriendsDrinksApi.newBuilder()
                .setApiType(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(request)
                .build();

        testDriver.pipeInput(inputFactory.create(friendsDrinksApiTopicName,
                new FriendsDrinksId(requestEvent.getCreateFriendsDrinksRequest().getAdminUserId()), requestEvent));

        while (true) {
            ProducerRecord<FriendsDrinksId, FriendsDrinksApi> event =
                    testDriver.readOutput(friendsDrinksApiTopicName, friendsDrinksIdDeserializer, friendsDrinksEventDeserializer);
            if (event != null) {
                if (event.value().getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                        event.value().getCreateFriendsDrinksResponse().getRequestId().equals(requestId)) {
                    output = event.value();
                }
            } else {
                break;
            }
        }
        // Now test we succeed.
        assertEquals(Result.SUCCESS, output.getCreateFriendsDrinksResponse().getResult());
    }

    @Test
    public void testDelete() {
        TestInputTopic<FriendsDrinksId, FriendsDrinksApi> inputTopic =
                testDriver.createInputTopic(
                        friendsDrinksApiTopicName,
                        friendsDrinksAvro.friendsDrinksIdSerializer(),
                        friendsDrinksAvro.friendsDrinksApiSerializer());

        FriendsDrinksApi deleteRequest = FriendsDrinksApi
                .newBuilder()
                .setApiType(ApiType.DELETE_FRIENDS_DRINKS_REQUEST)
                .setDeleteFriendsDrinksRequest(DeleteFriendsDrinksRequest
                        .newBuilder()
                        .setRequestId("1")
                        .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId("2").build())
                        .build())
                .build();
        inputTopic.pipeInput(deleteRequest);

        TestOutputTopic<FriendsDrinksId, FriendsDrinksApi> outputTopic =
                testDriver.createOutputTopic(
                        friendsDrinksApiTopicName,
                        friendsDrinksAvro.friendsDrinksIdDeserializer(),
                        friendsDrinksAvro.friendsDrinksApiDeserializer());

        FriendsDrinksApi outputValue = outputTopic.readValue();
        assertNotNull(outputValue.getDeleteFriendsDrinksResponse());
        assertEquals("1", outputValue.getDeleteFriendsDrinksResponse().getRequestId());
        assertEquals(Result.SUCCESS, outputValue.getDeleteFriendsDrinksResponse().getResult());
    }

    @AfterClass
    public static void cleanup() {
        testDriver.close();
    }

}
