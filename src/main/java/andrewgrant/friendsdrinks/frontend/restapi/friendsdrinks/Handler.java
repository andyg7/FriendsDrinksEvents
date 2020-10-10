package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import static andrewgrant.friendsdrinks.frontend.restapi.StreamsService.*;
import static andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post.PostFriendsDrinksMembershipRequestBean.*;
import static andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post.PostUsersRequestBean.CANCELLED_ACCOUNT;
import static andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post.PostUsersRequestBean.SIGNED_UP;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post.*;
import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;
import andrewgrant.friendsdrinks.user.avro.UserSignedUp;

/**
 * Implements frontend REST API.
 */
@Path("")
public class Handler {

    private static final Logger log = LoggerFactory.getLogger(Handler.class);

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
    public GetAllFriendsDrinksResponseBean getAllFriendsDrinks() {
        ReadOnlyKeyValueStore<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> allKvs = kv.all();
        List<String> friendsDrinksList = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> keyValue = allKvs.next();
            friendsDrinksList.add(keyValue.value.getFriendsDrinksId().getUuid());
        }
        allKvs.close();
        GetAllFriendsDrinksResponseBean response = new GetAllFriendsDrinksResponseBean();
        response.setFriendsDrinkList(friendsDrinksList);
        return response;
    }

    @GET
    @Path("/friendsdrinks/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetFriendsDrinksResponseBean getFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId) {
        ReadOnlyKeyValueStore<String, andrewgrant.friendsdrinks.avro.FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(friendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("%s does not exist", friendsDrinksId));
        }
        GetFriendsDrinksResponseBean response = new GetFriendsDrinksResponseBean();
        response.setFriendsDrinksId(friendsDrinksId);
        response.setName(friendsDrinksState.getName());
        response.setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId());
        return response;
    }

    @GET
    @Path("/users/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetUserResponseBean getUser(@PathParam("userId") String userId) {
        GetUserResponseBean getUserResponseBean = new GetUserResponseBean();

        ReadOnlyKeyValueStore<FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> friendsDrinksStore =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STORE, QueryableStoreTypes.keyValueStore()));

        ReadOnlyKeyValueStore<String, FriendsDrinksIdList> adminStore =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(ADMINS_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksIdList friendsDrinksIdList1 = adminStore.get(userId);
        if (friendsDrinksIdList1 != null && friendsDrinksIdList1.getIds() != null &&
                friendsDrinksIdList1.getIds().size() > 0) {
            List<FriendsDrinksBean> friendsDrinksBeans = new ArrayList<>();
            for (FriendsDrinksId drinksId : friendsDrinksIdList1.getIds()) {
                FriendsDrinksState state = friendsDrinksStore.get(drinksId);
                if (state == null || state.getFriendsDrinksId() == null) {
                    log.error(String.format("Failed to get FriendsDrinks with UUID %s and AdminUserId %s",
                            drinksId.getUuid(), drinksId.getAdminUserId()));
                    throw new RuntimeException();
                }
                FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
                friendsDrinksBean.setFriendsDrinksId(state.getFriendsDrinksId().getUuid());
                friendsDrinksBean.setName(state.getName());
                friendsDrinksBeans.add(friendsDrinksBean);
            }
            getUserResponseBean.setAdminFriendsDrinks(friendsDrinksBeans);
        } else {
            getUserResponseBean.setAdminFriendsDrinks(new ArrayList<>());
        }

        ReadOnlyKeyValueStore<String, FriendsDrinksIdList> membershipStore =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(MEMBERS_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksIdList friendsDrinksIdList2 = membershipStore.get(userId);
        if (friendsDrinksIdList2 != null && friendsDrinksIdList2.getIds() != null &&
                friendsDrinksIdList2.getIds().size() > 0) {
            List<FriendsDrinksBean> friendsDrinksBeans = new ArrayList<>();
            for (FriendsDrinksId drinksId : friendsDrinksIdList2.getIds()) {
                FriendsDrinksState state = friendsDrinksStore.get(drinksId);
                if (state == null || state.getFriendsDrinksId() == null) {
                    log.error(String.format("Failed to get FriendsDrinks with UUID %s and AdminUserId %s",
                            drinksId.getUuid(), drinksId.getAdminUserId()));
                    throw new RuntimeException();
                }
                FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
                friendsDrinksBean.setFriendsDrinksId(state.getFriendsDrinksId().getUuid());
                friendsDrinksBean.setName(state.getName());
                friendsDrinksBeans.add(friendsDrinksBean);
            }
            getUserResponseBean.setMemberFriendsDrinks(friendsDrinksBeans);
        } else {
            getUserResponseBean.setMemberFriendsDrinks(new ArrayList<>());
        }

        ReadOnlyKeyValueStore<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(PENDING_INVITATIONS_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> allKvs = kv.all();
        List<FriendsDrinksInvitationBean> invitationBeans = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> keyValue = allKvs.next();
            if (keyValue.key.getUserId().getUserId().equals(userId)) {
                FriendsDrinksInvitationBean invitationBean = new FriendsDrinksInvitationBean();
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
        String friendsDrinksId = UUID.randomUUID().toString();
        CreateFriendsDrinksRequest createFriendsDrinksRequest = CreateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(
                        FriendsDrinksId
                                .newBuilder()
                                .setUuid(friendsDrinksId)
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
        ProducerRecord<String, FriendsDrinksEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        FriendsDrinksEvent backendResponse = getApiResponse(requestId);
        CreateFriendsDrinksResponseBean responseBean = new CreateFriendsDrinksResponseBean();
        Result result = backendResponse.getCreateFriendsDrinksResponse().getResult();
        responseBean.setResult(result.name());
        responseBean.setFriendsDrinksId(friendsDrinksId);
        return responseBean;
    }

    @POST
    @Path("/users/{userId}/adminfriendsdrinks/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PostFriendsDrinksResponseBean updateFriendsDrinks(@PathParam("userId") String userId,
                                                             @PathParam("friendsDrinksId") String friendsDrinksId,
                                                             PostFriendsDrinksRequestBean requestBean)
            throws InterruptedException, ExecutionException {

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        FriendsDrinksEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setAdminUserId(userId)
                .setUuid(friendsDrinksId)
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

        ProducerRecord<String, FriendsDrinksEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        FriendsDrinksEvent backendResponse = getApiResponse(requestId);
        PostFriendsDrinksResponseBean responseBean = new PostFriendsDrinksResponseBean();
        responseBean.setResult(backendResponse.getUpdateFriendsDrinksResponse().getResult().name());
        return responseBean;
    }

    @DELETE
    @Path("/users/{userId}/adminfriendsdrinks/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public DeleteFriendsDrinksResponseBean deleteFriendsDrinks(
            @PathParam("userId") String userId,
            @PathParam("friendsDrinksId") String friendsDrinksId) throws InterruptedException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        DeleteFriendsDrinksRequest deleteFriendsDrinksRequest = DeleteFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(
                        FriendsDrinksId
                                .newBuilder()
                                .setUuid(friendsDrinksId)
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
        ProducerRecord<String, FriendsDrinksEvent> producerRecord = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(producerRecord);

        FriendsDrinksEvent backendResponse = getApiResponse(requestId);
        DeleteFriendsDrinksResponseBean responseBean = new DeleteFriendsDrinksResponseBean();
        Result result = backendResponse.getDeleteFriendsDrinksResponse().getResult();
        responseBean.setResult(result.name());
        return responseBean;
    }

    @POST
    @Path("/users/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PostUsersResponseBean postUsers(@PathParam("userId") String userId, PostUsersRequestBean requestBean)
            throws ExecutionException, InterruptedException {
        if (requestBean.getEventType().equals(SIGNED_UP) ||
                requestBean.getEventType().equals(CANCELLED_ACCOUNT)) {
            return registerUserEvent(userId, requestBean);
        } else {
            throw new RuntimeException(String.format("Unknown update type %s", requestBean.getEventType()));
        }
    }

    @POST
    @Path("/users/{userId}/friendsdrinks/{friendsDrinksId}/membership")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PostFriendsDrinksMembershipResponseBean postFriendsDrinksMembership(@PathParam("userId") String userId,
                                                             @PathParam("friendsDrinksId") String friendsDrinksId,
                                                             PostFriendsDrinksMembershipRequestBean requestBean)
            throws ExecutionException, InterruptedException {
        if (requestBean.getEventType().equals(ADD_USER)) {
            return handleAddUser(userId, friendsDrinksId, requestBean.getAddUserRequest());
        } else if (requestBean.getEventType().equals(REPLY_TO_INVITATION)) {
            return handleReplyToInvitation(userId, friendsDrinksId, requestBean.getReplyToInvitationRequest());
        } else if (requestBean.getEventType().equals(REMOVE_USER)) {
            return handleRemoveUser(userId, friendsDrinksId, requestBean.getRemoveUserRequest());
        } else {
            throw new RuntimeException(String.format("Unknown update type %s", requestBean.getEventType()));
        }
    }

    public PostFriendsDrinksMembershipResponseBean handleReplyToInvitation(String userId, String friendsDrinksId,
                                                         ReplyToInvitationRequestBean requestBean)
            throws InterruptedException, ExecutionException {

        ReadOnlyKeyValueStore<String, andrewgrant.friendsdrinks.avro.FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(friendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("FriendsDrinksId %s could not be found", friendsDrinksId));
        }

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        FriendsDrinksEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro;

        friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId())
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
                        .setReply(Reply.valueOf(requestBean.getResponse()))
                        .setRequestId(requestId)
                        .build();
        friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setRequestId(friendsDrinksInvitationReplyRequest.getRequestId())
                .setEventType(EventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST)
                .setFriendsDrinksInvitationReplyRequest(friendsDrinksInvitationReplyRequest)
                .build();

        ProducerRecord<String, FriendsDrinksEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        FriendsDrinksEvent backendResponse = getApiResponse(requestId);
        PostFriendsDrinksMembershipResponseBean responseBean = new PostFriendsDrinksMembershipResponseBean();
        Result result = backendResponse.getFriendsDrinksInvitationReplyResponse().getResult();
        responseBean.setResult(result.name());
        return responseBean;
    }

    public PostFriendsDrinksMembershipResponseBean handleRemoveUser(String userId, String friendsDrinksId,
                                                  RemoveUserRequestBean requestBean)
            throws InterruptedException, ExecutionException {

        ReadOnlyKeyValueStore<String, andrewgrant.friendsdrinks.avro.FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(friendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("FriendsDrinksId %s could not be found", friendsDrinksId));
        }

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        FriendsDrinksEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro;

        friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId())
                .setUuid(friendsDrinksId)
                .build();
        FriendsDrinksRemoveUserRequest removeUserRequest = FriendsDrinksRemoveUserRequest
                .newBuilder()
                .setUserIdToRemove(andrewgrant.friendsdrinks.api.avro.UserId
                        .newBuilder()
                        .setUserId(requestBean.getUserId())
                        .build())
                .setRequestId(requestId)
                .setFriendsDrinksId(friendsDrinksIdAvro)
                .setRequester(andrewgrant.friendsdrinks.api.avro.UserId
                        .newBuilder()
                        .setUserId(userId)
                        .build())
                .build();

        friendsDrinksEvent = FriendsDrinksEvent
                .newBuilder()
                .setRequestId(removeUserRequest.getRequestId())
                .setEventType(EventType.FRIENDSDRINKS_REMOVE_USER_REQUEST)
                .setFriendsDrinksRemoveUserRequest(removeUserRequest)
                .build();

        ProducerRecord<String, FriendsDrinksEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        FriendsDrinksEvent backendResponse = getApiResponse(requestId);
        PostFriendsDrinksMembershipResponseBean responseBean = new PostFriendsDrinksMembershipResponseBean();
        Result result = backendResponse.getFriendsDrinksRemoveUserResponse().getResult();
        responseBean.setResult(result.name());
        return responseBean;
    }

    public PostFriendsDrinksMembershipResponseBean handleAddUser(String userId, String friendsDrinksId,
                                                                 AddUserRequestBean requestBean) throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
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

        FriendsDrinksEvent backendResponse = getApiResponse(requestId);
        PostFriendsDrinksMembershipResponseBean responseBean = new PostFriendsDrinksMembershipResponseBean();
        Result result = backendResponse.getFriendsDrinksInvitationResponse().getResult();
        responseBean.setResult(result.name());
        return responseBean;
    }

    public PostUsersResponseBean registerUserEvent(String userId, PostUsersRequestBean requestBean) throws ExecutionException, InterruptedException {
        String eventType = requestBean.getEventType();
        final String topicName = envProps.getProperty("user-event.topic.name");
        UserId userIdAvro = UserId.newBuilder().setUserId(userId).build();
        UserEvent userEvent;
        if (eventType.equals(SIGNED_UP)) {
            UserSignedUp userSignedUp = UserSignedUp.newBuilder()
                    .setUserId(userIdAvro)
                    .setFirstName(requestBean.getFirstName())
                    .setLastName(requestBean.getLastName())
                    .build();
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
        ProducerRecord<UserId, UserEvent> record = new ProducerRecord<>(topicName, userEvent.getUserId(), userEvent);
        userKafkaProducer.send(record).get();
        PostUsersResponseBean postUsersResponseBean = new PostUsersResponseBean();
        postUsersResponseBean.setResult("SUCCESS");
        return postUsersResponseBean;
    }

    private FriendsDrinksEvent getApiResponse(String requestId) throws InterruptedException {
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

        return backendResponse;
    }

}
