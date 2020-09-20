package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import static andrewgrant.friendsdrinks.frontend.restapi.StreamsService.*;

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

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.CreatedFriendsDrinks;

/**
 * Implements frontend REST API friendsdrinks path.
 */
@Path("")
public class Handler {

    private KafkaStreams kafkaStreams;
    private KafkaProducer<FriendsDrinksId, FriendsDrinksEvent> kafkaProducer;
    private Properties envProps;

    public Handler(KafkaStreams kafkaStreams, KafkaProducer<FriendsDrinksId, FriendsDrinksEvent> kafkaProducer, Properties envProps) {
        this.kafkaStreams = kafkaStreams;
        this.kafkaProducer = kafkaProducer;
        this.envProps = envProps;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public GetAllFriendsDrinksResponseBean getAllFriendsDrinks() {
        ReadOnlyKeyValueStore<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> kv =
                kafkaStreams.store(FRIENDSDRINKS_STORE, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> allKvs = kv.all();
        List<FriendsDrinksBean> friendsDrinksList = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> keyValue = allKvs.next();
            CreatedFriendsDrinks friendsDrinks = keyValue.value.getCreatedFriendsDrinks();
            FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
            friendsDrinksBean.setAdminUserId(friendsDrinks.getAdminUserId());
            friendsDrinksBean.setId(keyValue.value.getFriendsDrinksId().getId());
            friendsDrinksBean.setName(friendsDrinks.getName());
            if (friendsDrinks.getUserIds() != null) {
                friendsDrinksBean.setUserIds(friendsDrinks.getUserIds().stream().collect(Collectors.toList()));
            }
            friendsDrinksList.add(friendsDrinksBean);
        }
        allKvs.close();
        GetAllFriendsDrinksResponseBean response = new GetAllFriendsDrinksResponseBean();
        response.setFriendsDrinkList(friendsDrinksList);
        return response;
    }

    @GET
    @Path("/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetFriendsDrinksResponseBean getFriendsDrink(@PathParam("userId") final String userId) {
        ReadOnlyKeyValueStore<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> kv =
                kafkaStreams.store(FRIENDSDRINKS_STORE, QueryableStoreTypes.keyValueStore());
        // TODO(andyg7): this is not efficient! We should have a state store that
        // removes the need for a full scan but for now this is OK.
        KeyValueIterator<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> allKvs = kv.all();
        List<FriendsDrinksBean> adminFriendsDrinks = new ArrayList<>();
        List<FriendsDrinksBean> memberFriendsDrinks = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> keyValue = allKvs.next();
            CreatedFriendsDrinks friendsDrinks = keyValue.value.getCreatedFriendsDrinks();
            if (friendsDrinks.getAdminUserId().equals(userId)) {
                FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
                friendsDrinksBean.setAdminUserId(friendsDrinks.getAdminUserId());
                friendsDrinksBean.setId(keyValue.value.getFriendsDrinksId().getId());
                friendsDrinksBean.setName(friendsDrinks.getName());
                if (friendsDrinks.getUserIds() != null) {
                    friendsDrinksBean.setUserIds(friendsDrinks.getUserIds().stream().collect(Collectors.toList()));
                }
                adminFriendsDrinks.add(friendsDrinksBean);
            } else {
                List<String> userIds = friendsDrinks.getUserIds();
                if (userIds.contains(userId)) {
                    FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
                    friendsDrinksBean.setAdminUserId(friendsDrinks.getAdminUserId());
                    friendsDrinksBean.setId(keyValue.value.getFriendsDrinksId().getId());
                    friendsDrinksBean.setName(friendsDrinks.getName());
                    friendsDrinksBean.setUserIds(friendsDrinks.getUserIds().stream().collect(Collectors.toList()));
                    memberFriendsDrinks.add(friendsDrinksBean);
                }
            }
        }
        allKvs.close();
        GetFriendsDrinksResponseBean response = new GetFriendsDrinksResponseBean();
        response.setAdminFriendsDrinks(adminFriendsDrinks);
        response.setMemberFriendsDrinks(memberFriendsDrinks);
        return response;
    }

    @DELETE
    @Path("/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public DeleteFriendsDrinksResponseBean deleteFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId) throws InterruptedException {
        final String topicName = envProps.getProperty("friendsdrinks_api.topic.name");
        String requestId = UUID.randomUUID().toString();
        DeleteFriendsDrinksRequest deleteFriendsDrinksRequest = DeleteFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId(friendsDrinksId).build())
                .setRequestId(requestId)
                .build();

        FriendsDrinksEvent friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setEventType(EventType.DELETE_FRIENDS_DRINKS_REQUEST)
                .setDeleteFriendsDrinksRequest(deleteFriendsDrinksRequest)
                .build();
        ProducerRecord<FriendsDrinksId, FriendsDrinksEvent> producerRecord =
                new ProducerRecord<>(topicName, friendsDrinksEvent.getDeleteFriendsDrinksRequest().getFriendsDrinksId(),
                        friendsDrinksEvent);
        kafkaProducer.send(producerRecord);

        ReadOnlyKeyValueStore<String, DeleteFriendsDrinksResponse> kv =
                kafkaStreams.store(DELETE_FRIENDSDRINKS_RESPONSES_STORE, QueryableStoreTypes.keyValueStore());

        DeleteFriendsDrinksResponse deleteFriendsDrinksResponse = kv.get(requestId);
        if (deleteFriendsDrinksResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (deleteFriendsDrinksResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(100);
                deleteFriendsDrinksResponse = kv.get(requestId);
            }
        }
        if (deleteFriendsDrinksResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get DeleteFriendsDrinksResponse for request id %s", requestId));
        }
        DeleteFriendsDrinksResponseBean responseBean = new DeleteFriendsDrinksResponseBean();
        Result result = deleteFriendsDrinksResponse.getResult();
        responseBean.setResult(result.toString());
        return responseBean;
    }

    @PUT
    @Path("/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public CreateFriendsDrinksResponseBean createFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId,
                                                               CreateFriendsDrinksRequestBean requestBean)
            throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks_api.topic.name");
        String requestId = UUID.randomUUID().toString();
        String scheduleType;
        if (requestBean.getScheduleType() != null) {
            scheduleType = requestBean.getScheduleType();
        } else {
            scheduleType = ScheduleType.OnDemand.name();
        }
        CreateFriendsDrinksRequest createFriendsDrinksRequest = CreateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId(friendsDrinksId).build())
                .setUserIds(requestBean.getUserIds().stream().collect(Collectors.toList()))
                .setAdminUserId(requestBean.getAdminUserId())
                .setScheduleType(ScheduleType.valueOf(scheduleType))
                .setCronSchedule(requestBean.getCronSchedule())
                .setRequestId(requestId)
                .setName(requestBean.getName())
                .build();
        FriendsDrinksEvent friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setEventType(EventType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(createFriendsDrinksRequest)
                .build();
        ProducerRecord<FriendsDrinksId, FriendsDrinksEvent> record =
                new ProducerRecord<>(
                        topicName,
                        friendsDrinksEvent.getCreateFriendsDrinksRequest().getFriendsDrinksId(),
                        friendsDrinksEvent);
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
                Thread.sleep(100);
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
