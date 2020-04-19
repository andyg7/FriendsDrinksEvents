package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import static andrewgrant.friendsdrinks.frontend.restapi.StreamsService.CREATE_FRIENDSDRINKS_RESPONSES_STORE;
import static andrewgrant.friendsdrinks.frontend.restapi.StreamsService.FRIENDSDRINKS_STORE;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.avro.*;

/**
 * Implements frontend REST API friendsdrinks path.
 */
@Path("")
public class Handler {

    private KafkaStreams kafkaStreams;
    private KafkaProducer<FriendsDrinksId, FriendsDrinksApi> kafkaProducer;
    private Properties envProps;

    public Handler(KafkaStreams kafkaStreams, KafkaProducer<FriendsDrinksId, FriendsDrinksApi> kafkaProducer, Properties envProps) {
        this.kafkaStreams = kafkaStreams;
        this.kafkaProducer = kafkaProducer;
        this.envProps = envProps;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public GetFriendsDrinksResponseBean getFriendsDrinksResponseBean() {
        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksEvent> kv =
                kafkaStreams.store(FRIENDSDRINKS_STORE, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<FriendsDrinksId, FriendsDrinksEvent> allKvs = kv.all();
        List<String> ids = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksId, FriendsDrinksEvent> keyValue = allKvs.next();
            ids.add(keyValue.value.getFriendsDrinksCreated().getFriendsDrinksId().getId());
        }
        allKvs.close();
        GetFriendsDrinksResponseBean response = new GetFriendsDrinksResponseBean();
        response.setIds(ids);
        return response;
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public CreateFriendsDrinksResponseBean createFriendsDrinks(CreateFriendsDrinksRequestBean requestBean)
            throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks_api.topic.name");
        String requestId = UUID.randomUUID().toString();
        String friendsDrinksId = UUID.randomUUID().toString();
        CreateFriendsDrinksRequest createFriendsDrinksRequest = CreateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId(friendsDrinksId).build())
                .setUserIds(requestBean.getUserIds().stream().collect(Collectors.toList()))
                .setAdminUserId(requestBean.getAdminUserId())
                .setScheduleType(ScheduleType.valueOf(requestBean.getScheduleType()))
                .setCronSchedule(requestBean.getCronSchedule())
                .setRequestId(requestId)
                .build();
        FriendsDrinksApi friendsDrinksApi = FriendsDrinksApi
                .newBuilder()
                .setApiType(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(createFriendsDrinksRequest)
                .build();
        ProducerRecord<FriendsDrinksId, FriendsDrinksApi> record =
                new ProducerRecord<>(
                        topicName,
                        friendsDrinksApi.getCreateFriendsDrinksRequest().getFriendsDrinksId(),
                        friendsDrinksApi);
        kafkaProducer.send(record).get();

        ReadOnlyKeyValueStore<String, CreateFriendsDrinksResponse> kv =
                kafkaStreams.store(CREATE_FRIENDSDRINKS_RESPONSES_STORE, QueryableStoreTypes.keyValueStore());

        CreateFriendsDrinksResponse createFriendsDrinksResponse = kv.get(requestId);
        if (createFriendsDrinksResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (createFriendsDrinksResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(500);
                createFriendsDrinksResponse = kv.get(requestId);
            }
        }
        if (createFriendsDrinksResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get CreateFriendsDrinksResponse for request id %s", requestId));
        }
        CreateFriendsDrinksResponseBean responseBean = new CreateFriendsDrinksResponseBean();
        Result result = createFriendsDrinksResponse.getResult();
        responseBean.setResult(result.toString());
        return responseBean;
    }
}
