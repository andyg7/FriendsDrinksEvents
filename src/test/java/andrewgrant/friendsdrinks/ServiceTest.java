package andrewgrant.friendsdrinks;

import static org.junit.Assert.assertEquals;
import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;
import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import andrewgrant.friendsdrinks.avro.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Tests FriendsDrinks service.
 */
public class ServiceTest {

    private static Properties envProps;
    private static String friendsDrinksTopicName;
    private static TopologyTestDriver testDriver;
    private static FriendsDrinksAvro avro;

    @BeforeClass
    public static void setup() throws IOException, RestClientException {
        envProps = load(TEST_CONFIG_FILE);
        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
        friendsDrinksTopicName = envProps.getProperty("friendsdrinks.topic.name");
        registryClient.register(friendsDrinksTopicName + "-key", Schema.create(Schema.Type.STRING));
        registryClient.register(friendsDrinksTopicName + "-value", FriendsDrinksEvent.getClassSchema());
        avro = new FriendsDrinksAvro(registryClient, envProps.getProperty("schema.registry.url"));

        Service service = new Service();
        Topology topology = service.buildTopology(envProps, avro);
        Properties streamsProps = service.buildStreamProperties(envProps);
        testDriver = new TopologyTestDriver(topology, streamsProps);
    }

    @Test
    public void testNewFriendsDrinksSuccess() {
        CreateFriendsDrinksRequest request = CreateFriendsDrinksRequest.newBuilder()
                .setFriendsDrinksId("friendsDrinksId")
                .setRequestId("reqId")
                .setUserIds(Arrays.asList("b", "c"))
                .setRequesterUserId("a")
                .setSchedule("blah")
                .build();
        FriendsDrinksEvent requestEvent = FriendsDrinksEvent.newBuilder()
                .setEventType(EventType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(request)
                .build();

        ConsumerRecordFactory<String, FriendsDrinksEvent> inputFactory =
                new ConsumerRecordFactory<>(Serdes.String().serializer(), avro.friendsDrinksEventSerializer());

        testDriver.pipeInput(inputFactory.create(friendsDrinksTopicName,
                requestEvent.getCreateFriendsDrinksRequest().getFriendsDrinksId(), requestEvent));

        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Deserializer<FriendsDrinksEvent> friendsDrinksEventDeserializer = avro.friendsDrinksEventDeserializer();
        ProducerRecord<String, FriendsDrinksEvent> output =
                testDriver.readOutput(friendsDrinksTopicName, stringDeserializer, friendsDrinksEventDeserializer);
        assertEquals(output.value().getEventType(), EventType.CREATE_FRIENDS_DRINKS_RESPONSE);
        assertEquals(output.value().getCreateFriendsDrinksResponse().getResult(), Result.SUCCESS);
    }

    @Test
    public void testNewFriendsDrinksFail() {
        String requesterUserId = "requester123";
        String requestId = "request123";
        CreateFriendsDrinksRequest request = CreateFriendsDrinksRequest.newBuilder()
                .setFriendsDrinksId("friendsDrinksId")
                .setRequestId(requestId)
                .setUserIds(Arrays.asList("b", "c"))
                .setRequesterUserId(requesterUserId)
                .setSchedule("blah")
                .build();
        FriendsDrinksEvent requestEvent = FriendsDrinksEvent.newBuilder()
                .setEventType(EventType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(request)
                .build();

        ConsumerRecordFactory<String, FriendsDrinksEvent> inputFactory =
                new ConsumerRecordFactory<>(Serdes.String().serializer(), avro.friendsDrinksEventSerializer());

        for (int i = 0; i < 5; i++) {
            CreateFriendsDrinksResponse response = CreateFriendsDrinksResponse.newBuilder()
                    .setRequestId(String.valueOf(i))
                    .setResult(Result.SUCCESS)
                    .build();
            FriendsDrinksEvent event = FriendsDrinksEvent.newBuilder()
                    .setEventType(EventType.CREATE_FRIENDS_DRINKS_RESPONSE)
                    .setCreateFriendsDrinksResponse(response)
                    .build();
            testDriver.pipeInput(inputFactory.create(friendsDrinksTopicName,
                    requesterUserId, event));
        }


        testDriver.pipeInput(inputFactory.create(friendsDrinksTopicName,
                requestEvent.getCreateFriendsDrinksRequest().getRequesterUserId(), requestEvent));

        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Deserializer<FriendsDrinksEvent> friendsDrinksEventDeserializer = avro.friendsDrinksEventDeserializer();
        FriendsDrinksEvent output = null;
        while (true) {
            ProducerRecord<String, FriendsDrinksEvent> event =
                    testDriver.readOutput(friendsDrinksTopicName, stringDeserializer, friendsDrinksEventDeserializer);
            if (event != null) {
                if (event.value().getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                        event.value().getCreateFriendsDrinksResponse().getRequestId().equals(requestId)) {
                    output = event.value();
                }
            } else {
                break;
            }
        }
        assertEquals(output.getCreateFriendsDrinksResponse().getResult(), Result.FAIL);
    }

    @AfterClass
    public static void cleanup() {
        testDriver.close();
    }

}
