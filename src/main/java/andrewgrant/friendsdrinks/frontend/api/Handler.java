package andrewgrant.friendsdrinks.frontend.api;

import static andrewgrant.friendsdrinks.frontend.MaterializedViewsService.*;
import static andrewgrant.friendsdrinks.frontend.api.membership.PostFriendsDrinksMembershipRequestBean.*;
import static andrewgrant.friendsdrinks.frontend.api.user.PostUsersRequestBean.*;

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
import java.util.stream.Collectors;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.frontend.api.friendsdrinks.*;
import andrewgrant.friendsdrinks.frontend.api.membership.*;
import andrewgrant.friendsdrinks.frontend.api.user.GetUsersResponseBean;
import andrewgrant.friendsdrinks.frontend.api.user.PostUsersRequestBean;
import andrewgrant.friendsdrinks.frontend.api.user.PostUsersResponseBean;
import andrewgrant.friendsdrinks.frontend.api.user.UserBean;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksInvitationState;
import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;
import andrewgrant.friendsdrinks.user.avro.UserLoggedIn;

/**
 * Implements frontend REST API.
 */
@Path("")
public class Handler {

    private static final Logger log = LoggerFactory.getLogger(Handler.class);

    private KafkaStreams kafkaStreams;
    private KafkaProducer<String, ApiEvent> friendsDrinksKafkaProducer;
    private KafkaProducer<UserId, UserEvent> userKafkaProducer;
    private Properties envProps;

    public Handler(KafkaStreams kafkaStreams,
                   KafkaProducer<String, ApiEvent> friendsDrinksKafkaProducer,
                   KafkaProducer<UserId, UserEvent> userKafkaProducer,
                   Properties envProps) {
        this.kafkaStreams = kafkaStreams;
        this.friendsDrinksKafkaProducer = friendsDrinksKafkaProducer;
        this.userKafkaProducer = userKafkaProducer;
        this.envProps = envProps;
    }

    @GET
    @Path("/users")
    @Produces(MediaType.APPLICATION_JSON)
    public GetUsersResponseBean getAllUsers() {
        ReadOnlyKeyValueStore<String, UserState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USERS_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<String, UserState> allKvs = kv.all();
        List<UserBean> users = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<String, UserState> keyValue = allKvs.next();
            UserBean userBean = new UserBean();
            UserState userState = keyValue.value;
            userBean.setEmail(userState.getEmail());
            userBean.setUserId(userState.getUserId().getUserId());
            userBean.setFirstName(userState.getFirstName());
            userBean.setLastName(userState.getLastName());
            users.add(userBean);
        }
        allKvs.close();
        GetUsersResponseBean response = new GetUsersResponseBean();
        response.setUsers(users);
        return response;
    }

    @GET
    @Path("/users/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public UserBean getUser(@PathParam("userId") String userId) {
        ReadOnlyKeyValueStore<String, UserState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USERS_STORE, QueryableStoreTypes.keyValueStore()));
        UserState userState = kv.get(userId);
        UserBean response = new UserBean();
        response.setEmail(userState.getEmail());
        response.setFirstName(userState.getFirstName());
        response.setLastName(userState.getLastName());
        response.setUserId(userState.getUserId().getUserId());
        return response;
    }

    @POST
    @Path("/users/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PostUsersResponseBean postUsers(@PathParam("userId") String userId, PostUsersRequestBean requestBean)
            throws ExecutionException, InterruptedException {
        return registerUserEvent(userId, requestBean);
    }

    @GET
    @Path("/friendsdrinkses")
    @Produces(MediaType.APPLICATION_JSON)
    public GetAllFriendsDrinksResponseBean getAllFriendsDrinks() {
        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<FriendsDrinksId, FriendsDrinksState> allKvs = kv.all();
        List<FriendsDrinksBean> friendsDrinksList = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksId, FriendsDrinksState> keyValue = allKvs.next();
            FriendsDrinksState friendsDrinksState = keyValue.value;
            FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
            friendsDrinksBean.setName(friendsDrinksState.getName());
            friendsDrinksBean.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
            friendsDrinksBean.setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId());
            friendsDrinksList.add(friendsDrinksBean);
        }
        allKvs.close();
        GetAllFriendsDrinksResponseBean response = new GetAllFriendsDrinksResponseBean();
        response.setFriendsDrinkList(friendsDrinksList);
        return response;
    }

    @GET
    @Path("/friendsdrinkses/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetFriendsDrinksResponseBean getFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId) {
        ReadOnlyKeyValueStore<String, FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(friendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("%s does not exist", friendsDrinksId));
        }
        FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
        friendsDrinksBean.setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId());
        friendsDrinksBean.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
        friendsDrinksBean.setName(friendsDrinksState.getName());

        GetFriendsDrinksResponseBean response = new GetFriendsDrinksResponseBean();
        response.setFriendsDrinks(friendsDrinksBean);
        return response;
    }

    @GET
    @Path("/friendsdrinksdetailpages/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetFriendsDrinksDetailPageResponseBean getFriendsDrinksDetailPage(@PathParam("friendsDrinksId") String friendsDrinksId) {
        ReadOnlyKeyValueStore<String, FriendsDrinksAggregate> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_DETAIL_PAGE_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksAggregate friendsDrinksAggregate = kv.get(friendsDrinksId);
        if (friendsDrinksAggregate == null) {
            throw new BadRequestException(String.format("%s does not exist", friendsDrinksId));
        }

        GetFriendsDrinksDetailPageResponseBean response = new GetFriendsDrinksDetailPageResponseBean();
        response.setAdminUserId(friendsDrinksAggregate.getFriendsDrinksId().getAdminUserId());
        response.setFriendsDrinksId(friendsDrinksAggregate.getFriendsDrinksId().getUuid());
        if (friendsDrinksAggregate.getMembers() != null) {
            response.setMembers(friendsDrinksAggregate.getMembers().stream().map(x -> {
                UserBean userBean = new UserBean();
                userBean.setUserId(x.getUserId().getUserId());
                userBean.setFirstName(x.getFirstName());
                userBean.setLastName(x.getLastName());
                userBean.setEmail(x.getEmail());
                return userBean;
            }).collect(Collectors.toList()));
        } else {
            response.setMembers(new ArrayList<>());
        }
        response.setName(friendsDrinksAggregate.getName());

        return response;
    }


    @GET
    @Path("/userhomepages/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetUserHomepageResponseBean getUserFriendsDrinksHomepage(@PathParam("userId") String userId) {
        GetUserHomepageResponseBean getUserHomepageResponseBean = new GetUserHomepageResponseBean();

        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksState> friendsDrinksStore =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STORE, QueryableStoreTypes.keyValueStore()));

        ReadOnlyKeyValueStore<String, FriendsDrinksIdList> adminStore =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(ADMINS_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksIdList adminFriendsDrinksIdList = adminStore.get(userId);

        if (adminFriendsDrinksIdList != null && adminFriendsDrinksIdList.getIds() != null &&
                adminFriendsDrinksIdList.getIds().size() > 0) {
            List<FriendsDrinksBean> friendsDrinksBeans = new ArrayList<>();
            for (FriendsDrinksId friendsDrinksId : adminFriendsDrinksIdList.getIds()) {
                FriendsDrinksState friendsDrinksState = friendsDrinksStore.get(friendsDrinksId);
                if (friendsDrinksState == null || friendsDrinksState.getFriendsDrinksId() == null) {
                    log.error("FriendsDrinks with uuid {} and adminUserId {} could not be found from {}",
                            friendsDrinksId.getUuid(),
                            friendsDrinksId.getAdminUserId(),
                            FRIENDSDRINKS_STORE);
                    continue;
                }
                if (friendsDrinksState.getStatus().equals(Status.DELETED)) {
                    continue;
                }
                FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
                friendsDrinksBean.setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId());
                friendsDrinksBean.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
                friendsDrinksBean.setName(friendsDrinksState.getName());
                friendsDrinksBeans.add(friendsDrinksBean);
            }
            getUserHomepageResponseBean.setAdminFriendsDrinksList(friendsDrinksBeans);
        } else {
            getUserHomepageResponseBean.setAdminFriendsDrinksList(new ArrayList<>());
        }

        ReadOnlyKeyValueStore<String, FriendsDrinksIdList> membershipStore =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(MEMBERS_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksIdList memberFriendsDrinksList = membershipStore.get(userId);
        if (memberFriendsDrinksList != null && memberFriendsDrinksList.getIds() != null &&
                memberFriendsDrinksList.getIds().size() > 0) {
            List<FriendsDrinksBean> friendsDrinksBeans = new ArrayList<>();
            for (FriendsDrinksId friendsDrinksId : memberFriendsDrinksList.getIds()) {
                FriendsDrinksState friendsDrinksState = friendsDrinksStore.get(friendsDrinksId);
                if (friendsDrinksState == null || friendsDrinksState.getFriendsDrinksId() == null) {
                    log.error("FriendsDrinks with uuid {} and adminUserId {} could not be found from {}",
                            friendsDrinksId.getUuid(),
                            friendsDrinksId.getAdminUserId(),
                            FRIENDSDRINKS_STORE);
                    continue;
                }
                if (friendsDrinksState.getStatus().equals(Status.DELETED)) {
                    continue;
                }
                FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
                friendsDrinksBean.setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId());
                friendsDrinksBean.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
                friendsDrinksBean.setName(friendsDrinksState.getName());
                friendsDrinksBeans.add(friendsDrinksBean);
            }
            getUserHomepageResponseBean.setMemberFriendsDrinksList(friendsDrinksBeans);
        } else {
            getUserHomepageResponseBean.setMemberFriendsDrinksList(new ArrayList<>());
        }

        ReadOnlyKeyValueStore<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksInvitationState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(INVITATIONS_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksInvitationState> allKvs = kv.all();
        List<FriendsDrinksInvitationBean> invitationBeans = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksInvitationState> keyValue = allKvs.next();
            if (keyValue.key.getUserId().getUserId().equals(userId)) {
                FriendsDrinksInvitationBean invitationBean = new FriendsDrinksInvitationBean();
                invitationBean.setFriendsDrinksId(
                        keyValue.value.getMembershipId().getFriendsDrinksId().getUuid());
                invitationBean.setMessage(keyValue.value.getMessage());
                FriendsDrinksState friendsDrinksState = friendsDrinksStore.get(FriendsDrinksId
                        .newBuilder()
                        .setUuid(keyValue.value.getMembershipId().getFriendsDrinksId().getUuid())
                        .setAdminUserId(keyValue.value.getMembershipId().getFriendsDrinksId().getAdminUserId())
                        .build());
                invitationBean.setFriendsDrinksName(friendsDrinksState.getName());
                invitationBeans.add(invitationBean);
            }
        }
        allKvs.close();
        getUserHomepageResponseBean.setInvitations(invitationBeans);

        return getUserHomepageResponseBean;
    }

    @GET
    @Path("/friendsdrinksinvitations/users/{userId}/friendsdrinkses/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public FriendsDrinksInvitationBean getInvitation(@PathParam("userId") String userId,
                                                     @PathParam("friendsDrinksId") String friendsDrinksId) {
        ReadOnlyKeyValueStore<String, FriendsDrinksState> fdKv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = fdKv.get(friendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("friendsDrinksId %s could not be found", friendsDrinksId));
        }

        ReadOnlyKeyValueStore<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksInvitationState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(INVITATIONS_STORE, QueryableStoreTypes.keyValueStore()));
        andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId invitationId =
                andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId
                .newBuilder()
                .setFriendsDrinksId(andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId
                        .newBuilder()
                        .setUuid(friendsDrinksId)
                        .setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId())
                        .build())
                .setUserId(andrewgrant.friendsdrinks.membership.avro.UserId.newBuilder().setUserId(userId).build())
                .build();

        FriendsDrinksInvitationState invitation = kv.get(invitationId);
        if (invitation == null) {
            throw new BadRequestException(String.format("Invitation for userId %s and friendsDrinksId %s could not be found",
                    userId, friendsDrinksId));
        }

        FriendsDrinksInvitationBean response = new FriendsDrinksInvitationBean();
        response.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
        response.setFriendsDrinksName(friendsDrinksState.getName());
        response.setMessage(invitation.getMessage());

        return response;
    }

    @POST
    @Path("/friendsdrinkses")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public CreateFriendsDrinksResponseBean createFriendsDrinks(CreateFriendsDrinksRequestBean requestBean)
            throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        String friendsDrinksId = UUID.randomUUID().toString();

        FriendsDrinksId friendsDrinksIdAvro =
                FriendsDrinksId
                        .newBuilder()
                        .setUuid(friendsDrinksId)
                        .setAdminUserId(requestBean.getAdminUserId())
                        .build();
        CreateFriendsDrinksRequest createFriendsDrinksRequest = CreateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(friendsDrinksIdAvro)
                .setRequestId(requestId)
                .setName(requestBean.getName())
                .build();

        ApiEvent friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(createFriendsDrinksRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_EVENT)
                .setFriendsDrinksEvent(FriendsDrinksEvent.newBuilder()
                        .setEventType(FriendsDrinksEventType.CREATE_FRIENDSDRINKS_REQUEST)
                        .setCreateFriendsDrinksRequest(createFriendsDrinksRequest)
                        .setRequestId(createFriendsDrinksRequest.getRequestId())
                        .setFriendsDrinksId(friendsDrinksIdAvro)
                        .build())
                .build();
        ProducerRecord<String, ApiEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        ApiEvent backendResponse = getApiResponse(requestId);
        CreateFriendsDrinksResponseBean responseBean = new CreateFriendsDrinksResponseBean();
        Result result = backendResponse.getFriendsDrinksEvent().getCreateFriendsDrinksResponse().getResult();
        responseBean.setResult(result.name());
        responseBean.setFriendsDrinksId(friendsDrinksId);
        return responseBean;
    }

    @POST
    @Path("/friendsdrinkses/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public UpdateFriendsDrinksResponseBean updateFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId,
                                                               UpdateFriendsDrinksRequestBean requestBean)
            throws InterruptedException, ExecutionException {

        ReadOnlyKeyValueStore<String, FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(friendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("FriendsDrinksId %s could not be found", friendsDrinksId));
        }

        String adminUserId = friendsDrinksState.getFriendsDrinksId().getAdminUserId();

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        ApiEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setAdminUserId(adminUserId)
                .setUuid(friendsDrinksId)
                .build();
        UpdateFriendsDrinksRequest updateFriendsDrinksRequest = UpdateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(friendsDrinksIdAvro)
                .setUpdateType(UpdateType.valueOf(UpdateType.PARTIAL.name()))
                .setRequestId(requestId)
                .setName(requestBean.getName())
                .build();
        friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(updateFriendsDrinksRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_EVENT)
                .setFriendsDrinksEvent(FriendsDrinksEvent.newBuilder()
                        .setEventType(FriendsDrinksEventType.UPDATE_FRIENDSDRINKS_REQUEST)
                        .setUpdateFriendsDrinksRequest(updateFriendsDrinksRequest)
                        .setRequestId(updateFriendsDrinksRequest.getRequestId())
                        .setFriendsDrinksId(friendsDrinksIdAvro)
                        .build())
                .build();

        ProducerRecord<String, ApiEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        ApiEvent backendResponse = getApiResponse(requestId);
        UpdateFriendsDrinksResponseBean responseBean = new UpdateFriendsDrinksResponseBean();
        responseBean.setResult(backendResponse.getFriendsDrinksEvent().getUpdateFriendsDrinksResponse().getResult().name());
        return responseBean;
    }

    @DELETE
    @Path("/friendsdrinkses/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public DeleteFriendsDrinksResponseBean deleteFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId)
            throws InterruptedException {

        ReadOnlyKeyValueStore<String, FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(friendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("FriendsDrinksId %s could not be found", friendsDrinksId));
        }

        String adminUserId = friendsDrinksState.getFriendsDrinksId().getAdminUserId();
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        FriendsDrinksId friendsDrinksIdAvro =
                FriendsDrinksId
                        .newBuilder()
                        .setUuid(friendsDrinksId)
                        .setAdminUserId(adminUserId)
                        .build();
        DeleteFriendsDrinksRequest deleteFriendsDrinksRequest = DeleteFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(friendsDrinksIdAvro)
                .setRequestId(requestId)
                .build();

        ApiEvent friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(deleteFriendsDrinksRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_EVENT)
                .setFriendsDrinksEvent(FriendsDrinksEvent.newBuilder()
                        .setEventType(FriendsDrinksEventType.DELETE_FRIENDSDRINKS_REQUEST)
                        .setDeleteFriendsDrinksRequest(deleteFriendsDrinksRequest)
                        .setRequestId(deleteFriendsDrinksRequest.getRequestId())
                        .setFriendsDrinksId(friendsDrinksIdAvro)
                        .build())
                .build();
        ProducerRecord<String, ApiEvent> producerRecord = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(producerRecord);

        ApiEvent backendResponse = getApiResponse(requestId);
        DeleteFriendsDrinksResponseBean responseBean = new DeleteFriendsDrinksResponseBean();
        Result result = backendResponse.getFriendsDrinksEvent().getDeleteFriendsDrinksResponse().getResult();
        responseBean.setResult(result.name());
        return responseBean;
    }

    @POST
    @Path("/friendsdrinksmemberships")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PostFriendsDrinksMembershipResponseBean postFriendsDrinksMembership(PostFriendsDrinksMembershipRequestBean requestBean)
            throws ExecutionException, InterruptedException {
        String userId = requestBean.getUserId();
        String friendsDrinksId = requestBean.getFriendsDrinksId();
        if (requestBean.getRequestType().equals(INVITE_USER)) {
            return handleInviteUser(userId, friendsDrinksId, requestBean.getInviteUserRequest());
        } else if (requestBean.getRequestType().equals(REPLY_TO_INVITATION)) {
            return handleReplyToInvitation(userId, friendsDrinksId, requestBean.getReplyToInvitationRequest());
        } else {
            throw new RuntimeException(String.format("Unknown update type %s", requestBean.getRequestType()));
        }
    }

    public PostFriendsDrinksMembershipResponseBean handleReplyToInvitation(String userId, String friendsDrinksId,
                                                         ReplyToInvitationRequestBean requestBean)
            throws InterruptedException, ExecutionException {

        ReadOnlyKeyValueStore<String, FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_KEYED_BY_SINGLE_ID_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(friendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("FriendsDrinksId %s could not be found", friendsDrinksId));
        }

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        ApiEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro;

        friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setAdminUserId(friendsDrinksState.getFriendsDrinksId().getAdminUserId())
                .setUuid(friendsDrinksId)
                .build();
        FriendsDrinksInvitationReplyRequest friendsDrinksInvitationReplyRequest =
                FriendsDrinksInvitationReplyRequest.newBuilder()
                        .setMembershipId(FriendsDrinksMembershipId.newBuilder()
                                .setFriendsDrinksId(friendsDrinksIdAvro)
                                .setUserId(
                                        andrewgrant.friendsdrinks.api.avro.UserId
                                                .newBuilder()
                                                .setUserId(userId)
                                                .build()
                                )
                                .build())
                        .setReply(Reply.valueOf(requestBean.getResponse()))
                        .setRequestId(requestId)
                        .build();
        friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(friendsDrinksInvitationReplyRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT)
                .setFriendsDrinksMembershipEvent(FriendsDrinksMembershipEvent.newBuilder()
                        .setEventType(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST)
                        .setFriendsDrinksInvitationReplyRequest(friendsDrinksInvitationReplyRequest)
                        .setRequestId(friendsDrinksInvitationReplyRequest.getRequestId())
                        .setMembershipId(FriendsDrinksMembershipId.newBuilder()
                                .setFriendsDrinksId(friendsDrinksIdAvro)
                                .setUserId(
                                        andrewgrant.friendsdrinks.api.avro.UserId
                                                .newBuilder()
                                                .setUserId(userId)
                                                .build()
                                )
                                .build())
                        .build())
                .build();

        ProducerRecord<String, ApiEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        ApiEvent backendResponse = getApiResponse(requestId);
        PostFriendsDrinksMembershipResponseBean responseBean = new PostFriendsDrinksMembershipResponseBean();
        Result result = backendResponse.getFriendsDrinksMembershipEvent().getFriendsDrinksInvitationReplyResponse().getResult();
        responseBean.setResult(result.name());
        return responseBean;
    }

    public PostFriendsDrinksMembershipResponseBean handleInviteUser(String userId, String friendsDrinksId,
                                                                    InviteUserRequestBean requestBean)
            throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        ApiEvent friendsDrinksEvent;
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
                        .setMembershipId(FriendsDrinksMembershipId.newBuilder()
                                .setFriendsDrinksId(friendsDrinksIdAvro)
                                .setUserId(
                                        andrewgrant.friendsdrinks.api.avro.UserId
                                                .newBuilder()
                                                .setUserId(requestBean.getUserId())
                                                .build())
                                .build())
                        .build();
        friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(friendsDrinksInvitationRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT)
                .setFriendsDrinksMembershipEvent(FriendsDrinksMembershipEvent.newBuilder()
                        .setEventType(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REQUEST)
                        .setFriendsDrinksInvitationRequest(friendsDrinksInvitationRequest)
                        .setRequestId(friendsDrinksInvitationRequest.getRequestId())
                        .setMembershipId(FriendsDrinksMembershipId.newBuilder()
                                .setFriendsDrinksId(friendsDrinksIdAvro)
                                .setUserId(
                                        andrewgrant.friendsdrinks.api.avro.UserId
                                                .newBuilder()
                                                .setUserId(requestBean.getUserId())
                                                .build())
                                .build())
                        .build())
                .build();

        ProducerRecord<String, ApiEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        ApiEvent backendResponse = getApiResponse(requestId);
        PostFriendsDrinksMembershipResponseBean responseBean = new PostFriendsDrinksMembershipResponseBean();
        Result result = backendResponse.getFriendsDrinksMembershipEvent().getFriendsDrinksInvitationResponse().getResult();
        responseBean.setResult(result.name());
        return responseBean;
    }

    public PostUsersResponseBean registerUserEvent(String userId, PostUsersRequestBean requestBean) throws ExecutionException, InterruptedException {
        String eventType = requestBean.getEventType();
        final String topicName = envProps.getProperty("user-event.topic.name");
        UserId userIdAvro = UserId.newBuilder().setUserId(userId).build();
        String requestId = UUID.randomUUID().toString();
        UserEvent userEvent;
        if (eventType.equals(LOGGED_IN)) {
            userEvent = UserEvent
                    .newBuilder()
                    .setRequestId(requestId)
                    .setEventType(andrewgrant.friendsdrinks.user.avro.EventType.LOGGED_IN)
                    .setUserLoggedIn(UserLoggedIn
                            .newBuilder()
                            .setFirstName(requestBean.getLoggedInEvent().getFirstName())
                            .setLastName(requestBean.getLoggedInEvent().getLastName())
                            .setEmail(requestBean.getLoggedInEvent().getEmail())
                            .setUserId(userIdAvro)
                            .build())
                    .setUserId(userIdAvro)
                    .build();
        } else if (eventType.equals(LOGGED_OUT)) {
            userEvent = UserEvent
                    .newBuilder()
                    .setRequestId(requestId)
                    .setEventType(andrewgrant.friendsdrinks.user.avro.EventType.LOGGED_OUT)
                    .setUserId(userIdAvro)
                    .build();
        } else if (eventType.equals(SIGNED_OUT_SESSION_EXPIRED)) {
            userEvent = UserEvent
                    .newBuilder()
                    .setRequestId(requestId)
                    .setEventType(andrewgrant.friendsdrinks.user.avro.EventType.SIGNED_OUT_SESSION_EXPIRED)
                    .setUserId(userIdAvro)
                    .build();
        } else if (eventType.equals(DELETED)) {
            userEvent = UserEvent
                    .newBuilder()
                    .setRequestId(requestId)
                    .setEventType(andrewgrant.friendsdrinks.user.avro.EventType.DELETED)
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

    private ApiEvent getApiResponse(String requestId) throws InterruptedException {
        ReadOnlyKeyValueStore<String, ApiEvent> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(RESPONSES_STORE, QueryableStoreTypes.keyValueStore()));
        ApiEvent backendResponse = kv.get(requestId);
        if (backendResponse == null) {
            for (int i = 0; i < 50; i++) {
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
                    "Failed to get API response for request id %s", requestId));
        }

        return backendResponse;
    }

}
