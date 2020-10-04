package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import static andrewgrant.friendsdrinks.frontend.restapi.StreamsService.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
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
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post.PostFriendsDrinksRequestBean;
import andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post.PostFriendsDrinksResponseBean;
import andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post.PostUsersRequestBean;
import andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post.PostUsersResponseBean;
import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;
import andrewgrant.friendsdrinks.user.avro.UserSignedUp;

/**
 * Implements frontend REST API.
 */
@Path("")
public class Handler {

    public static final String INVITE_FRIEND = "INVITE_FRIEND";
    public static final String REPLY_TO_INVITATION = "REPLY_TO_INVITATION";
    public static final String SIGNED_UP = "SIGNED_UP";
    public static final String CANCELLED_ACCOUNT = "CANCELLED_ACCOUNT";

    private KafkaStreams kafkaStreams;
    private KafkaProducer<String, FriendsDrinksEvent> friendsDrinksKafkaProducer;
    private KafkaProducer<UserId, UserEvent> userKafkaProducer;
    private Properties envProps;

    public Handler(KafkaStreams kafkaStreams,
                   KafkaProducer<String, FriendsDrinksEvent> friendsDrinksKafkaProducer,
                   KafkaProducer<UserId, UserEvent> userKafkaProducer,
                   Properties envProps) {
        this.kafkaStreams = kafkaStreams;
        this.friendsDrinksKafkaProducer = friendsDrinksKafkaProducer;
        this.userKafkaProducer = userKafkaProducer;
        this.envProps = envProps;
    }

    @GET
    @Path("/friendsdrinks")
    @Produces(MediaType.APPLICATION_JSON)
    public GetAllFriendsDrinksResponseBean getAllFriendsDrinks(@QueryParam("adminUserId") String adminUserId,
                                                               @QueryParam("memberUserId") String memberUserId) {
        ReadOnlyKeyValueStore<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> allKvs = kv.all();
        List<FriendsDrinksIdBean> friendsDrinksList = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> keyValue = allKvs.next();
            FriendsDrinksState friendsDrinksState = keyValue.value;
            if (adminUserId != null && !friendsDrinksState.getFriendsDrinksId().getAdminUserId().equals(adminUserId)) {
                continue;
            }
            if (memberUserId != null) {
                List<String> userIds = friendsDrinksState.getUserIds();
                if (userIds == null || !userIds.contains(memberUserId)) {
                    continue;
                }
            }
            friendsDrinksList.add(new FriendsDrinksIdBean(
                    keyValue.value.getFriendsDrinksId().getAdminUserId(),
                    keyValue.value.getFriendsDrinksId().getUuid()));
        }
        allKvs.close();
        GetAllFriendsDrinksResponseBean response = new GetAllFriendsDrinksResponseBean();
        response.setFriendsDrinkList(friendsDrinksList);
        return response;
    }

    @GET
    @Path("/friendsdrinks/{friendsDrinksUuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetFriendsDrinksResponseBean getFriendsDrinks(@PathParam("friendsDrinksUuid") String friendsDrinksUuid) {
        ReadOnlyKeyValueStore<String, andrewgrant.friendsdrinks.avro.FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(friendsDrinksUuid);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("%s does not exist", friendsDrinksUuid));
        }
        GetFriendsDrinksResponseBean response = new GetFriendsDrinksResponseBean();
        response.setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId());
        response.setId(friendsDrinksState.getFriendsDrinksId().getUuid());
        response.setName(friendsDrinksState.getName());

        if (friendsDrinksState.getUserIds() != null) {
            response.setUserIds(friendsDrinksState.getUserIds().stream().collect(Collectors.toList()));
        }
        return response;
    }

    @GET
    @Path("/users/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetUserResponseBean getUser(@PathParam("userId") String userId) {
        GetAllFriendsDrinksResponseBean adminFriendsDrinks = getAllFriendsDrinks(userId, null);
        GetUserResponseBean getUserResponseBean = new GetUserResponseBean();
        getUserResponseBean.setAdminFriendsDrinksIds(adminFriendsDrinks.getFriendsDrinkList());
        GetAllFriendsDrinksResponseBean memberFriendsDrinks = getAllFriendsDrinks(null, userId);
        getUserResponseBean.setMemberFriendsDrinksIds(memberFriendsDrinks.getFriendsDrinkList());

        ReadOnlyKeyValueStore<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(PENDING_INVITATIONS_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> allKvs = kv.all();
        List<FriendsDrinksInvitationBean> invitationBeans = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> keyValue = allKvs.next();
            if (keyValue.key.getUserId().getUserId().equals(userId)) {
                FriendsDrinksInvitationBean invitationBean = new FriendsDrinksInvitationBean();
                invitationBean.setAdminUserId(keyValue.value.getFriendsDrinksId().getAdminUserId());
                invitationBean.setFriendsDrinksId(keyValue.value.getFriendsDrinksId().getUuid());
                invitationBean.setMessage(keyValue.value.getMessage());
                invitationBeans.add(invitationBean);
            }
        }
        allKvs.close();
        getUserResponseBean.setInvitations(invitationBeans);

        return getUserResponseBean;
    }

    @POST
    @Path("/users/{userId}/adminfriendsdrinks")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public CreateFriendsDrinksResponseBean createFriendsDrinks(@PathParam("userId") String userId,
                                                               CreateFriendsDrinksRequestBean requestBean)
            throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        String friendsDrinksUuid = UUID.randomUUID().toString();
        CreateFriendsDrinksRequest createFriendsDrinksRequest = CreateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(
                        FriendsDrinksId
                                .newBuilder()
                                .setUuid(friendsDrinksUuid)
                                .setAdminUserId(userId)
                                .build())
                .setRequestId(requestId)
                .setName(requestBean.getName())
                .build();
        FriendsDrinksEvent friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setRequestId(createFriendsDrinksRequest.getRequestId())
                .setEventType(EventType.CREATE_FRIENDSDRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(createFriendsDrinksRequest)
                .build();
        ProducerRecord<String, FriendsDrinksEvent> record =
                new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        ReadOnlyKeyValueStore<String, FriendsDrinksEvent> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(RESPONSES_STORE, QueryableStoreTypes.keyValueStore()));

        FriendsDrinksEvent backendResponse = kv.get(requestId);
        if (backendResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (backendResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(100);
                backendResponse = kv.get(requestId);
            }
        }
        if (backendResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get CreateFriendsDrinksResponse for request id %s", requestId));
        }
        CreateFriendsDrinksResponseBean responseBean = new CreateFriendsDrinksResponseBean();
        Result result = backendResponse.getCreateFriendsDrinksResponse().getResult();
        responseBean.setResult(result.toString());
        FriendsDrinksIdBean friendsDrinksIdBean = new FriendsDrinksIdBean();
        friendsDrinksIdBean.setUuid(friendsDrinksUuid);
        friendsDrinksIdBean.setAdminUserId(userId);
        responseBean.setFriendsDrinksId(friendsDrinksIdBean);
        return responseBean;
    }

    @POST
    @Path("/users/{userId}/friendsdrinks/{friendsDrinksUuid}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PostFriendsDrinksResponseBean updateFriendsDrinks(@PathParam("userId") String userId,
                                                             @PathParam("friendsDrinksUuid") String friendsDrinksUuid,
                                                             PostFriendsDrinksRequestBean requestBean)
            throws InterruptedException, ExecutionException {

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        FriendsDrinksEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setAdminUserId(userId)
                .setUuid(friendsDrinksUuid)
                .build();
        UpdateFriendsDrinksRequest updateFriendsDrinksRequest = UpdateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(friendsDrinksIdAvro)
                .setUpdateType(UpdateType.valueOf(UpdateType.PARTIAL.name()))
                .setRequestId(requestId)
                .setName(requestBean.getName())
                .build();
        friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setRequestId(updateFriendsDrinksRequest.getRequestId())
                .setEventType(EventType.UPDATE_FRIENDSDRINKS_REQUEST)
                .setUpdateFriendsDrinksRequest(updateFriendsDrinksRequest)
                .build();

        ProducerRecord<String, FriendsDrinksEvent> record =
                new ProducerRecord<>(
                        topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        ReadOnlyKeyValueStore<String, FriendsDrinksEvent> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(RESPONSES_STORE, QueryableStoreTypes.keyValueStore()));

        FriendsDrinksEvent backendResponse = kv.get(requestId);
        if (backendResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (backendResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(100);
                backendResponse = kv.get(requestId);
            }
        }
        if (backendResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get UpdateFriendsDrinksResponse for request id %s", requestId));
        }

        PostFriendsDrinksResponseBean responseBean = new PostFriendsDrinksResponseBean();
        responseBean.setResult(backendResponse.getUpdateFriendsDrinksResponse().getResult().toString());
        return responseBean;
    }

    @DELETE
    @Path("/users/{userId}/adminfriendsdrinks/{friendsDrinksUuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public DeleteFriendsDrinksResponseBean deleteFriendsDrinks(
            @PathParam("userId") String userId,
            @PathParam("friendsDrinksUuid") String friendsDrinksUuid) throws InterruptedException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        DeleteFriendsDrinksRequest deleteFriendsDrinksRequest = DeleteFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(
                        FriendsDrinksId
                                .newBuilder()
                                .setUuid(friendsDrinksUuid)
                                .setAdminUserId(userId)
                                .build())
                .setRequestId(requestId)
                .build();

        FriendsDrinksEvent friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setRequestId(deleteFriendsDrinksRequest.getRequestId())
                .setEventType(EventType.DELETE_FRIENDSDRINKS_REQUEST)
                .setDeleteFriendsDrinksRequest(deleteFriendsDrinksRequest)
                .build();
        ProducerRecord<String, FriendsDrinksEvent> producerRecord =
                new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(producerRecord);

        ReadOnlyKeyValueStore<String, FriendsDrinksEvent> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(RESPONSES_STORE, QueryableStoreTypes.keyValueStore()));

        FriendsDrinksEvent backendResponse = kv.get(requestId);
        if (backendResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (backendResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(100);
                backendResponse = kv.get(requestId);
            }
        }
        if (backendResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get DeleteFriendsDrinksResponse for request id %s", requestId));
        }
        DeleteFriendsDrinksResponseBean responseBean = new DeleteFriendsDrinksResponseBean();
        Result result = backendResponse.getDeleteFriendsDrinksResponse().getResult();
        responseBean.setResult(result.toString());
        return responseBean;
    }

    @POST
    @Path("/users/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PostUsersResponseBean postUsers(@PathParam("userId") String userId,
                                           PostUsersRequestBean requestBean)
            throws ExecutionException, InterruptedException {
        if (requestBean.getEventType().equals(SIGNED_UP) ||
                requestBean.getEventType().equals(CANCELLED_ACCOUNT)) {
            return registerUserEvent(userId, requestBean.getEventType());
        }

        if (requestBean.getEventType().equals(INVITE_FRIEND)) {
            return handleInviteFriend(userId, requestBean);
        } else if (requestBean.getEventType().equals(REPLY_TO_INVITATION)) {
            return handleReplyToInvitation(userId, requestBean);
        } else {
            throw new RuntimeException(String.format("Unknown update type %s", requestBean.getEventType()));
        }
    }

    public PostUsersResponseBean handleReplyToInvitation(String userId, PostUsersRequestBean requestBean)
            throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        String friendsDrinksId = requestBean.getFriendsDrinksUuid();
        FriendsDrinksEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro;

        friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setAdminUserId(requestBean.getAdminUserId())
                .setUuid(friendsDrinksId)
                .build();
        FriendsDrinksInvitationReplyRequest friendsDrinksInvitationReplyRequest =
                FriendsDrinksInvitationReplyRequest.newBuilder()
                        .setFriendsDrinksId(friendsDrinksIdAvro)
                        .setUserId(
                                andrewgrant.friendsdrinks.api.avro.UserId
                                        .newBuilder()
                                        .setUserId(userId)
                                        .build()
                        )
                        .setReply(Reply.valueOf(requestBean.getInvitationReply()))
                        .setRequestId(requestId)
                        .build();
        friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setRequestId(friendsDrinksInvitationReplyRequest.getRequestId())
                .setEventType(EventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST)
                .setFriendsDrinksInvitationReplyRequest(friendsDrinksInvitationReplyRequest)
                .build();

        ProducerRecord<String, FriendsDrinksEvent> record =
                new ProducerRecord<>(
                        topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        ReadOnlyKeyValueStore<String, FriendsDrinksEvent> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(RESPONSES_STORE, QueryableStoreTypes.keyValueStore()));

        FriendsDrinksEvent backendResponse = kv.get(requestId);
        if (backendResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (backendResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(100);
                backendResponse = kv.get(requestId);
            }
        }
        if (backendResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get backend response for request id %s", requestId));
        }

        PostUsersResponseBean responseBean = new PostUsersResponseBean();
        Result result = backendResponse.getFriendsDrinksInvitationReplyResponse().getResult();
        responseBean.setResult(result.toString());
        return responseBean;
    }

    public PostUsersResponseBean handleInviteFriend(String userId, PostUsersRequestBean requestBean) throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        String friendsDrinksId = requestBean.getFriendsDrinksUuid();
        FriendsDrinksEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro;

        friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setAdminUserId(userId)
                .setUuid(friendsDrinksId)
                .build();
        FriendsDrinksInvitationRequest friendsDrinksInvitationRequest =
                FriendsDrinksInvitationRequest
                        .newBuilder()
                        .setRequestId(requestId)
                        .setFriendsDrinksId(friendsDrinksIdAvro)
                        .setUserId(
                                andrewgrant.friendsdrinks.api.avro.UserId
                                        .newBuilder()
                                        .setUserId(requestBean.getUserId())
                                        .build())
                        .build();
        friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setRequestId(friendsDrinksInvitationRequest.getRequestId())
                .setEventType(EventType.FRIENDSDRINKS_INVITATION_REQUEST)
                .setFriendsDrinksInvitationRequest(friendsDrinksInvitationRequest)
                .build();

        ProducerRecord<String, FriendsDrinksEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();
        ReadOnlyKeyValueStore<String, FriendsDrinksEvent> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(RESPONSES_STORE, QueryableStoreTypes.keyValueStore()));

        FriendsDrinksEvent backendResponse = kv.get(requestId);
        if (backendResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (backendResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(100);
                backendResponse = kv.get(requestId);
            }
        }
        if (backendResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get backend response for request id %s", requestId));
        }

        PostUsersResponseBean responseBean = new PostUsersResponseBean();
        Result result = backendResponse.getFriendsDrinksInvitationResponse().getResult();
        responseBean.setResult(result.toString());
        return responseBean;
    }

    public PostUsersResponseBean registerUserEvent(String userId, String eventType) throws ExecutionException, InterruptedException {
        final String topicName = envProps.getProperty("user-event.topic.name");
        UserId userIdAvro = UserId.newBuilder().setUserId(userId).build();
        UserEvent userEvent;
        if (eventType.equals(SIGNED_UP)) {
            UserSignedUp userSignedUp = UserSignedUp.newBuilder().setUserId(userIdAvro).build();
            userEvent = UserEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.user.avro.EventType.SIGNED_UP)
                    .setUserSignedUp(userSignedUp)
                    .setUserId(userIdAvro)
                    .build();
        } else if (eventType.equals(CANCELLED_ACCOUNT)) {
            userEvent = UserEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.user.avro.EventType.CANCELLED_ACCOUNT)
                    .setUserId(userIdAvro)
                    .build();
        } else {
            throw new RuntimeException(String.format("Unknown event type %s", eventType));
        }
        ProducerRecord<UserId, UserEvent> record = new ProducerRecord<>(
                topicName,
                userEvent.getUserId(),
                userEvent
        );
        userKafkaProducer.send(record).get();
        PostUsersResponseBean postUsersResponseBean = new PostUsersResponseBean();
        postUsersResponseBean.setResult("SUCCESS");
        return postUsersResponseBean;
    }

}
