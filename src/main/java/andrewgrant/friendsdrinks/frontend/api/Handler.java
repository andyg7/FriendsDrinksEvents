package andrewgrant.friendsdrinks.frontend.api;

import static andrewgrant.friendsdrinks.frontend.api.membership.PostFriendsDrinksMembershipRequestBean.*;
import static andrewgrant.friendsdrinks.frontend.api.user.PostUsersRequestBean.*;
import static andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.frontend.api.friendsdrinks.*;
import andrewgrant.friendsdrinks.frontend.api.meetup.ScheduleFriendsDrinksMeetupRequestBean;
import andrewgrant.friendsdrinks.frontend.api.meetup.ScheduleFriendsDrinksMeetupResponseBean;
import andrewgrant.friendsdrinks.frontend.api.membership.*;
import andrewgrant.friendsdrinks.frontend.api.statestorebeans.*;
import andrewgrant.friendsdrinks.frontend.api.user.GetUsersResponseBean;
import andrewgrant.friendsdrinks.frontend.api.user.PostUsersRequestBean;
import andrewgrant.friendsdrinks.frontend.api.user.PostUsersResponseBean;
import andrewgrant.friendsdrinks.frontend.api.user.UserBean;
import andrewgrant.friendsdrinks.frontend.kafkastreams.LocalStateRetriever;

/**
 * Implements frontend REST API.
 */
@Path("")
public class Handler {

    private static final Logger log = LoggerFactory.getLogger(Handler.class);

    private KafkaStreams kafkaStreams;
    private KafkaProducer<String, ApiEvent> friendsDrinksKafkaProducer;
    private KafkaProducer<FriendsDrinksMeetupId, FriendsDrinksMeetupEvent> friendsDrinksMeetupKafkaProducer;
    private KafkaProducer<UserId, UserEvent> userKafkaProducer;
    private Properties envProps;
    private StateRetriever stateRetriever;
    private LocalStateRetriever localStateRetriever;

    public Handler(KafkaStreams kafkaStreams,
                   KafkaProducer<String, ApiEvent> friendsDrinksKafkaProducer,
                   KafkaProducer<UserId, UserEvent> userKafkaProducer,
                   KafkaProducer<FriendsDrinksMeetupId, FriendsDrinksMeetupEvent> friendsDrinksMeetupKafkaProducer,
                   Properties envProps, StateRetriever stateRetriever) {
        this.kafkaStreams = kafkaStreams;
        this.friendsDrinksKafkaProducer = friendsDrinksKafkaProducer;
        this.userKafkaProducer = userKafkaProducer;
        this.friendsDrinksMeetupKafkaProducer = friendsDrinksMeetupKafkaProducer;
        this.envProps = envProps;
        localStateRetriever = new LocalStateRetriever(kafkaStreams);
        this.stateRetriever = stateRetriever;
    }

    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public HealthCheckResponseBean healthCheck() {
        KafkaStreams.State state = kafkaStreams.state();
        if (!state.isRunningOrRebalancing()) {
            throw new RuntimeException(String.format("State is %s", state.name()));
        }
        HealthCheckResponseBean healthCheckResponseBean = new HealthCheckResponseBean();
        healthCheckResponseBean.setStatus("HEALTHY");
        return healthCheckResponseBean;
    }

    @GET
    @Path("/users")
    @Produces(MediaType.APPLICATION_JSON)
    public GetUsersResponseBean getAllUsers() {
        List<UserStateBean> userStateBeanList = stateRetriever.getAllUserStates();
        GetUsersResponseBean response = new GetUsersResponseBean();
        response.setUsers(userStateBeanList);
        return response;
    }

    @GET
    @Path("/users/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public UserBean getUser(@PathParam("userId") String userId) {
        UserStateBean userStateBean = stateRetriever.getUserState(userId);
        UserBean response = new UserBean();
        response.setEmail(userStateBean.getEmail());
        response.setFirstName(userStateBean.getFirstName());
        response.setLastName(userStateBean.getLastName());
        response.setUserId(userStateBean.getUserId());
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
        List<FriendsDrinksStateBean> friendsDrinksStateBeanList = stateRetriever.getAllFriendsDrinksStates();
        GetAllFriendsDrinksResponseBean response = new GetAllFriendsDrinksResponseBean();
        response.setFriendsDrinkList(friendsDrinksStateBeanList);
        return response;
    }

    @GET
    @Path("/friendsdrinkses/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetFriendsDrinksResponseBean getFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId) {
        FriendsDrinksStateBean friendsDrinksStateBean = stateRetriever.getFriendsDrinksState(friendsDrinksId);
        if (friendsDrinksStateBean == null) {
            throw new BadRequestException(String.format("%s does not exist", friendsDrinksId));
        }
        FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
        friendsDrinksBean.setAdminUserId(friendsDrinksStateBean.getAdminUserId());
        friendsDrinksBean.setFriendsDrinksId(friendsDrinksStateBean.getFriendsDrinksId());
        friendsDrinksBean.setName(friendsDrinksStateBean.getName());

        GetFriendsDrinksResponseBean response = new GetFriendsDrinksResponseBean();
        response.setFriendsDrinks(friendsDrinksBean);
        return response;
    }

    @GET
    @Path("/friendsdrinksdetailpages/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public FriendsDrinksDetailPageBean getFriendsDrinksDetailPage(@PathParam("friendsDrinksId") String friendsDrinksId) {
        FriendsDrinksDetailPageBean friendsDrinksDetailPageBean = stateRetriever.getFriendsDrinksDetailPage(friendsDrinksId);
        if (friendsDrinksDetailPageBean == null) {
            throw new BadRequestException(String.format("Friends drinks %s could not be found", friendsDrinksId));
        }
        return friendsDrinksDetailPageBean;
    }

    @GET
    @Path("/userhomepages/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public UserHomepageBean getUserFriendsDrinksHomepage(@PathParam("userId") String userId) {
        UserHomepageBean userHomepage = stateRetriever.getUserHomePage(userId);
        if (userHomepage == null) {
            throw new BadRequestException(String.format("User ID %s could not be found", userId));
        }

        return userHomepage;
    }

    @GET
    @Path("/friendsdrinksinvitations/users/{userId}/friendsdrinkses/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public FriendsDrinksInvitationBean getInvitation(@PathParam("userId") String userId,
                                                     @PathParam("friendsDrinksId") String friendsDrinksId) {

        ReadOnlyKeyValueStore<FriendsDrinksMembershipId, InvitationStateFriendsDrinksEnriched> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(INVITATIONS_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksMembershipId invitationId =
                FriendsDrinksMembershipId
                .newBuilder()
                .setFriendsDrinksId(FriendsDrinksId
                        .newBuilder()
                        .setUuid(friendsDrinksId)
                        .build())
                .setUserId(UserId.newBuilder().setUserId(userId).build())
                .build();

        InvitationStateFriendsDrinksEnriched invitation = kv.get(invitationId);
        if (invitation == null) {
            throw new BadRequestException(String.format("Invitation for userId %s and friendsDrinksId %s could not be found",
                    userId, friendsDrinksId));
        }

        FriendsDrinksInvitationBean response = new FriendsDrinksInvitationBean();
        response.setFriendsDrinksId(invitation.getMembershipId().getFriendsDrinksId().getUuid());
        response.setFriendsDrinksName(invitation.getFriendsDrinksState().getName());
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
                        .build();
        CreateFriendsDrinksRequest createFriendsDrinksRequest = CreateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(friendsDrinksIdAvro)
                .setRequestId(requestId)
                .setName(requestBean.getName())
                .setAdminUserId(requestBean.getAdminUserId())
                .build();

        ApiEvent friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(createFriendsDrinksRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_EVENT)
                .setFriendsDrinksEvent(FriendsDrinksApiEvent.newBuilder()
                        .setEventType(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_REQUEST)
                        .setCreateFriendsDrinksRequest(createFriendsDrinksRequest)
                        .setRequestId(createFriendsDrinksRequest.getRequestId())
                        .setFriendsDrinksId(friendsDrinksIdAvro)
                        .build())
                .build();
        ProducerRecord<String, ApiEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        CreateFriendsDrinksResponseBean responseBean = new CreateFriendsDrinksResponseBean();
        responseBean.setResult(waitAndGetApiResponse(requestId).getResult());
        responseBean.setFriendsDrinksId(friendsDrinksId);
        return responseBean;
    }

    @POST
    @Path("/friendsdrinkses/{friendsDrinksId}/meetups")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public ScheduleFriendsDrinksMeetupResponseBean scheduleFriendsDrinksMeetup(@PathParam("friendsDrinksId") String friendsDrinksId,
                                                                               ScheduleFriendsDrinksMeetupRequestBean requestBean)
            throws ExecutionException, InterruptedException {
        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksId avroFriendsDrinksId = FriendsDrinksId.newBuilder().setUuid(friendsDrinksId).build();
        FriendsDrinksState friendsDrinksState = kv.get(avroFriendsDrinksId);
        if (friendsDrinksState == null) {
            throw new BadRequestException(String.format("%s does not exist", avroFriendsDrinksId.getUuid()));
        }
        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksMembershipIdList> memberships =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(MEMBERSHIP_FRIENDSDRINKS_ID_STORE, QueryableStoreTypes.keyValueStore()));
        List<UserId> userIds = new ArrayList<>();
        FriendsDrinksMembershipIdList membershipIdList = memberships.get(avroFriendsDrinksId);
        if (membershipIdList != null) {
            for (FriendsDrinksMembershipId membershipId : membershipIdList.getIds()) {
                userIds.add(UserId.newBuilder().setUserId(membershipId.getUserId().getUserId()).build());
            }
        }
        userIds.add(UserId.newBuilder().setUserId(friendsDrinksState.getAdminUserId()).build());
        String meetupId = UUID.randomUUID().toString();
        FriendsDrinksMeetupScheduled friendsDrinksMeetupScheduled = FriendsDrinksMeetupScheduled
                .newBuilder()
                .setMeetupId(FriendsDrinksMeetupId.newBuilder().setUuid(meetupId).build())
                .setFriendsDrinksId(avroFriendsDrinksId)
                .setUserIds(userIds)
                .setDate(requestBean.getDate())
                .setRequestId(UUID.randomUUID().toString())
                .build();
        FriendsDrinksMeetupEvent friendsDrinksMeetupEvent = FriendsDrinksMeetupEvent
                .newBuilder()
                .setEventType(FriendsDrinksMeetupEventType.SCHEDULED)
                .setMeetupId(FriendsDrinksMeetupId.newBuilder().setUuid(meetupId).build())
                .setMeetupScheduled(friendsDrinksMeetupScheduled)
                .build();

        final String topicName = envProps.getProperty("friendsdrinks-meetup-event.topic.name");
        ProducerRecord<FriendsDrinksMeetupId, FriendsDrinksMeetupEvent> record = new ProducerRecord<>(topicName,
                friendsDrinksMeetupEvent.getMeetupId(), friendsDrinksMeetupEvent);
        friendsDrinksMeetupKafkaProducer.send(record).get();
        return new ScheduleFriendsDrinksMeetupResponseBean();
    }

    @POST
    @Path("/friendsdrinkses/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public UpdateFriendsDrinksResponseBean updateFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId,
                                                               UpdateFriendsDrinksRequestBean requestBean)
            throws InterruptedException, ExecutionException {

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        ApiEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setUuid(friendsDrinksId)
                .build();
        UpdateFriendsDrinksRequest updateFriendsDrinksRequest = UpdateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(friendsDrinksIdAvro)
                .setRequestId(requestId)
                .setName(requestBean.getName())
                .build();
        friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(updateFriendsDrinksRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_EVENT)
                .setFriendsDrinksEvent(FriendsDrinksApiEvent.newBuilder()
                        .setEventType(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_REQUEST)
                        .setUpdateFriendsDrinksRequest(updateFriendsDrinksRequest)
                        .setRequestId(updateFriendsDrinksRequest.getRequestId())
                        .setFriendsDrinksId(friendsDrinksIdAvro)
                        .build())
                .build();

        ProducerRecord<String, ApiEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        UpdateFriendsDrinksResponseBean responseBean = new UpdateFriendsDrinksResponseBean();
        responseBean.setResult(waitAndGetApiResponse(requestId).getResult());
        return responseBean;
    }

    @DELETE
    @Path("/friendsdrinkses/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public DeleteFriendsDrinksResponseBean deleteFriendsDrinks(@PathParam("friendsDrinksId") String friendsDrinksId)
            throws InterruptedException {

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        FriendsDrinksId friendsDrinksIdAvro =
                FriendsDrinksId
                        .newBuilder()
                        .setUuid(friendsDrinksId)
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
                .setFriendsDrinksEvent(FriendsDrinksApiEvent.newBuilder()
                        .setEventType(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_REQUEST)
                        .setDeleteFriendsDrinksRequest(deleteFriendsDrinksRequest)
                        .setRequestId(deleteFriendsDrinksRequest.getRequestId())
                        .setFriendsDrinksId(friendsDrinksIdAvro)
                        .build())
                .build();
        ProducerRecord<String, ApiEvent> producerRecord = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(producerRecord);

        DeleteFriendsDrinksResponseBean responseBean = new DeleteFriendsDrinksResponseBean();
        responseBean.setResult(waitAndGetApiResponse(requestId).getResult());
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
            return handleInviteUser(friendsDrinksId, requestBean.getInviteUserRequest());
        } else if (requestBean.getRequestType().equals(REPLY_TO_INVITATION)) {
            return handleReplyToInvitation(userId, friendsDrinksId, requestBean.getReplyToInvitationRequest());
        } else {
            throw new RuntimeException(String.format("Unknown update type %s", requestBean.getRequestType()));
        }
    }

    public PostFriendsDrinksMembershipResponseBean handleReplyToInvitation(String userId, String friendsDrinksId,
                                                         ReplyToInvitationRequestBean requestBean)
            throws InterruptedException, ExecutionException {

        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        ApiEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro;

        friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setUuid(friendsDrinksId)
                .build();
        FriendsDrinksInvitationReplyRequest friendsDrinksInvitationReplyRequest =
                FriendsDrinksInvitationReplyRequest.newBuilder()
                        .setMembershipId(FriendsDrinksMembershipId.newBuilder()
                                .setFriendsDrinksId(friendsDrinksIdAvro)
                                .setUserId(
                                        UserId
                                                .newBuilder()
                                                .setUserId(userId)
                                                .build()
                                )
                                .build())
                        .setReply(FriendsDrinksInvitationReply.valueOf(requestBean.getResponse()))
                        .setRequestId(requestId)
                        .build();
        friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(friendsDrinksInvitationReplyRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT)
                .setFriendsDrinksMembershipEvent(FriendsDrinksMembershipApiEvent.newBuilder()
                        .setEventType(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST)
                        .setFriendsDrinksInvitationReplyRequest(friendsDrinksInvitationReplyRequest)
                        .setRequestId(friendsDrinksInvitationReplyRequest.getRequestId())
                        .setMembershipId(FriendsDrinksMembershipId.newBuilder()
                                .setFriendsDrinksId(friendsDrinksIdAvro)
                                .setUserId(
                                        UserId
                                                .newBuilder()
                                                .setUserId(userId)
                                                .build()
                                )
                                .build())
                        .build())
                .build();

        ProducerRecord<String, ApiEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        PostFriendsDrinksMembershipResponseBean responseBean = new PostFriendsDrinksMembershipResponseBean();
        responseBean.setResult(waitAndGetApiResponse(requestId).getResult());
        return responseBean;
    }

    public PostFriendsDrinksMembershipResponseBean handleInviteUser(String friendsDrinksId,
                                                                    InviteUserRequestBean requestBean)
            throws InterruptedException, ExecutionException {
        final String topicName = envProps.getProperty("friendsdrinks-api.topic.name");
        String requestId = UUID.randomUUID().toString();
        ApiEvent friendsDrinksEvent;
        FriendsDrinksId friendsDrinksIdAvro;

        friendsDrinksIdAvro = FriendsDrinksId
                .newBuilder()
                .setUuid(friendsDrinksId)
                .build();
        FriendsDrinksInvitationRequest friendsDrinksInvitationRequest =
                FriendsDrinksInvitationRequest
                        .newBuilder()
                        .setRequestId(requestId)
                        .setMembershipId(FriendsDrinksMembershipId.newBuilder()
                                .setFriendsDrinksId(friendsDrinksIdAvro)
                                .setUserId(
                                        andrewgrant.friendsdrinks.avro.UserId
                                                .newBuilder()
                                                .setUserId(requestBean.getUserId())
                                                .build())
                                .build())
                        .build();
        friendsDrinksEvent = ApiEvent
                .newBuilder()
                .setRequestId(friendsDrinksInvitationRequest.getRequestId())
                .setEventType(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT)
                .setFriendsDrinksMembershipEvent(FriendsDrinksMembershipApiEvent.newBuilder()
                        .setEventType(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REQUEST)
                        .setFriendsDrinksInvitationRequest(friendsDrinksInvitationRequest)
                        .setRequestId(friendsDrinksInvitationRequest.getRequestId())
                        .setMembershipId(FriendsDrinksMembershipId.newBuilder()
                                .setFriendsDrinksId(friendsDrinksIdAvro)
                                .setUserId(
                                        UserId
                                                .newBuilder()
                                                .setUserId(requestBean.getUserId())
                                                .build())
                                .build())
                        .build())
                .build();

        ProducerRecord<String, ApiEvent> record = new ProducerRecord<>(topicName, requestId, friendsDrinksEvent);
        friendsDrinksKafkaProducer.send(record).get();

        PostFriendsDrinksMembershipResponseBean responseBean = new PostFriendsDrinksMembershipResponseBean();
        responseBean.setResult(waitAndGetApiResponse(requestId).getResult());
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
                    .setEventType(UserEventType.LOGGED_IN)
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
                    .setEventType(UserEventType.LOGGED_OUT)
                    .setUserId(userIdAvro)
                    .build();
        } else if (eventType.equals(SIGNED_OUT_SESSION_EXPIRED)) {
            userEvent = UserEvent
                    .newBuilder()
                    .setRequestId(requestId)
                    .setEventType(UserEventType.SIGNED_OUT_SESSION_EXPIRED)
                    .setUserId(userIdAvro)
                    .build();
        } else if (eventType.equals(DELETED)) {
            userEvent = UserEvent
                    .newBuilder()
                    .setRequestId(requestId)
                    .setEventType(UserEventType.DELETED)
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

    private ApiResponseBean waitAndGetApiResponse(String requestId) throws InterruptedException {
        ApiResponseBean backendResponse = stateRetriever.getApiResponse(requestId);
        if (backendResponse == null) {
            for (int i = 0; i < 50; i++) {
                if (backendResponse != null) {
                    break;
                }
                // Give the backend some time and try again.
                Thread.sleep(100);
                backendResponse = stateRetriever.getApiResponse(requestId);
            }
        }
        if (backendResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get API response for request id %s", requestId));
        }

        return backendResponse;
    }

    // APIs that interact with local state.

    @GET
    @Path("/friendsdrinks-state-store/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public FriendsDrinksStateBean getFriendsDrinksStateBean(@PathParam("friendsDrinksId") String friendsDrinksId) {
        return localStateRetriever.getFriendsDrinksState(friendsDrinksId);
    }

    @GET
    @Path("/friendsdrinks-state-store")
    @Produces(MediaType.APPLICATION_JSON)
    public List<FriendsDrinksStateBean> getAllFriendsDrinksStateBeans() {
        return localStateRetriever.getAllFriendsDrinksStates();
    }

    @GET
    @Path("/users-state-store/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public UserStateBean getUserStateBean(@PathParam("userId") String userId) {
        return localStateRetriever.getUserState(userId);
    }

    @GET
    @Path("/users-state-store")
    @Produces(MediaType.APPLICATION_JSON)
    public List<UserStateBean> getAllUserStateBeans() {
        return localStateRetriever.getAllUserStates();
    }

    @GET
    @Path("/api-response-state-store/{requestId}")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponseBean getApiResponse(@PathParam("requestId") String requestId) {
        return localStateRetriever.getApiResponse(requestId);
    }

    @GET
    @Path("/user-homepages-state-store/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public UserHomepageBean getUserHomepage(@PathParam("userId") String userId) {
        return localStateRetriever.getUserHomePage(userId);
    }

    @GET
    @Path("/friendsdrinks-detail-page-state-store/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public FriendsDrinksDetailPageBean getLocalFriendsDrinksDetailPage(@PathParam("friendsDrinksId") String uuid) {
        return localStateRetriever.getFriendsDrinksDetailPage(uuid);
    }

}
