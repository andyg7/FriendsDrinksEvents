package andrewgrant.friendsdrinks.frontend.api;

import static andrewgrant.friendsdrinks.frontend.api.membership.PostFriendsDrinksMembershipRequestBean.*;
import static andrewgrant.friendsdrinks.frontend.api.user.PostUsersRequestBean.*;
import static andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService.*;

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

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.frontend.api.friendsdrinks.*;
import andrewgrant.friendsdrinks.frontend.api.meetup.MeetupBean;
import andrewgrant.friendsdrinks.frontend.api.meetup.ScheduleFriendsDrinksMeetupRequestBean;
import andrewgrant.friendsdrinks.frontend.api.meetup.ScheduleFriendsDrinksMeetupResponseBean;
import andrewgrant.friendsdrinks.frontend.api.membership.*;
import andrewgrant.friendsdrinks.frontend.api.state.FriendsDrinksStateBean;
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
        ReadOnlyKeyValueStore<String, UserState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USERS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
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
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USERS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
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
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<FriendsDrinksId, FriendsDrinksState> allKvs = kv.all();
        List<FriendsDrinksBean> friendsDrinksList = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksId, FriendsDrinksState> keyValue = allKvs.next();
            FriendsDrinksState friendsDrinksState = keyValue.value;
            FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
            friendsDrinksBean.setName(friendsDrinksState.getName());
            friendsDrinksBean.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
            friendsDrinksBean.setAdminUserId(friendsDrinksState.getAdminUserId());
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
        FriendsDrinksStateBean friendsDrinksStateBean = stateRetriever.getFriendsDrinksState(friendsDrinksId);
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
    public GetFriendsDrinksDetailPageResponseBean getFriendsDrinksDetailPage(@PathParam("friendsDrinksId") String friendsDrinksId) {
        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksDetailPage> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_DETAIL_PAGE_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksDetailPage friendsDrinkDetailPage = kv.get(FriendsDrinksId.newBuilder().setUuid(friendsDrinksId).build());
        if (friendsDrinkDetailPage == null || friendsDrinkDetailPage.getStatus().equals(FriendsDrinksStatus.DELETED)) {
            throw new BadRequestException(String.format("%s does not exist", friendsDrinksId));
        }

        GetFriendsDrinksDetailPageResponseBean response = new GetFriendsDrinksDetailPageResponseBean();
        response.setAdminUserId(friendsDrinkDetailPage.getAdminUserId());
        response.setFriendsDrinksId(friendsDrinkDetailPage.getFriendsDrinksId().getUuid());
        if (friendsDrinkDetailPage.getMembers() != null) {
            response.setMembers(friendsDrinkDetailPage.getMembers().stream().map(x -> {
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

        ReadOnlyKeyValueStore<String, UserState> usersKv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USERS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        if (friendsDrinkDetailPage.getMeetups() != null) {
            response.setMeetups(friendsDrinkDetailPage.getMeetups().stream().map(x -> {
                MeetupBean meetupBean = new MeetupBean();
                meetupBean.setDate(x.getDate());
                meetupBean.setUsers(x.getUserIds().stream().map(z-> {
                    UserBean userBean = new UserBean();
                    userBean.setUserId(z.getUserId());
                    UserState userState = usersKv.get(z.getUserId());
                    if (userState == null) {
                        log.error(String.format("Failed to get user state for %s", z.getUserId()));
                    } else {
                        userBean.setEmail(userState.getEmail());
                        userBean.setFirstName(userState.getFirstName());
                        userBean.setLastName(userState.getLastName());
                    }
                    return userBean;
                }).collect(Collectors.toList()));
                return meetupBean;
            }).collect(Collectors.toList()));
        } else {
            response.setMeetups(new ArrayList<>());
        }
        response.setName(friendsDrinkDetailPage.getName());

        return response;
    }


    @GET
    @Path("/userhomepages/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetUserHomepageResponseBean getUserFriendsDrinksHomepage(@PathParam("userId") String userId) {
        GetUserHomepageResponseBean getUserHomepageResponseBean = new GetUserHomepageResponseBean();

        ReadOnlyKeyValueStore<String, UserHomepage> userHomepageStore =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USER_HOMEPAGES_STATE_STORE, QueryableStoreTypes.keyValueStore()));

        UserHomepage userHomepage = userHomepageStore.get(userId);
        if (userHomepage == null) {
            throw new BadRequestException(String.format("User ID %s could not be found", userId));
        }

        if (userHomepage.getAdminFriendsDrinks() != null &&
                userHomepage.getAdminFriendsDrinks().getFriendsDrinks() != null &&
                userHomepage.getAdminFriendsDrinks().getFriendsDrinks().size() > 0) {
            List<FriendsDrinksState> friendsDrinksStates = userHomepage.getAdminFriendsDrinks().getFriendsDrinks();
            getUserHomepageResponseBean.setAdminFriendsDrinksList(friendsDrinksStates.stream().map(x -> {
                FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
                friendsDrinksBean.setFriendsDrinksId(x.getFriendsDrinksId().getUuid());
                friendsDrinksBean.setAdminUserId(x.getAdminUserId());
                friendsDrinksBean.setName(x.getName());
                return friendsDrinksBean;
            }).collect(Collectors.toList()));
        }

        if (userHomepage.getMemberFriendsDrinks() != null &&
                userHomepage.getMemberFriendsDrinks().getFriendsDrinks() != null &&
                userHomepage.getMemberFriendsDrinks().getFriendsDrinks().size() > 0) {
            List<FriendsDrinksState> friendsDrinksStates = userHomepage.getMemberFriendsDrinks().getFriendsDrinks();
            getUserHomepageResponseBean.setMemberFriendsDrinksList(friendsDrinksStates.stream().map(x -> {
                FriendsDrinksBean friendsDrinksBean = new FriendsDrinksBean();
                friendsDrinksBean.setFriendsDrinksId(x.getFriendsDrinksId().getUuid());
                friendsDrinksBean.setAdminUserId(x.getAdminUserId());
                friendsDrinksBean.setName(x.getName());
                return friendsDrinksBean;
            }).collect(Collectors.toList()));
        }

        if (userHomepage.getInvitations() != null &&
                userHomepage.getInvitations().getInvitations() != null &&
                userHomepage.getInvitations().getInvitations().size() > 0) {
            List<InvitationStateFriendsDrinksEnriched> invitations = userHomepage.getInvitations().getInvitations();
            getUserHomepageResponseBean.setInvitations(invitations.stream().map(x -> {
                FriendsDrinksInvitationBean friendsDrinksInvitationBean = new FriendsDrinksInvitationBean();
                friendsDrinksInvitationBean.setFriendsDrinksId(x.getMembershipId().getFriendsDrinksId().getUuid());
                friendsDrinksInvitationBean.setMessage(x.getMessage());
                friendsDrinksInvitationBean.setFriendsDrinksName(x.getFriendsDrinksState().getName());
                return friendsDrinksInvitationBean;
            }).collect(Collectors.toList()));
        }

        return getUserHomepageResponseBean;
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

        ApiEvent backendResponse = getApiResponse(requestId);
        CreateFriendsDrinksResponseBean responseBean = new CreateFriendsDrinksResponseBean();
        Result result = backendResponse.getFriendsDrinksEvent().getCreateFriendsDrinksResponse().getResult();
        responseBean.setResult(result.name());
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

    private ApiEvent getApiResponse(String requestId) throws InterruptedException {
        ReadOnlyKeyValueStore<String, ApiEvent> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(RESPONSES_STATE_STORE, QueryableStoreTypes.keyValueStore()));
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

    // APIs that interact with local state.
    @GET
    @Path("/friendsdrinks-state-store/{friendsDrinksId}")
    @Produces(MediaType.APPLICATION_JSON)
    public FriendsDrinksStateBean getFriendsDrinksStateBean(@PathParam("friendsDrinksId") String friendsDrinksId) {
        return localStateRetriever.getFriendsDrinksState(friendsDrinksId);
    }

}
