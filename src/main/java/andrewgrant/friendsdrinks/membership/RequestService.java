package andrewgrant.friendsdrinks.membership;

import static andrewgrant.friendsdrinks.TopicNameConfigKey.FRIENDSDRINKS_STATE;
import static andrewgrant.friendsdrinks.env.Properties.load;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;
import static andrewgrant.friendsdrinks.user.TopicNameConfigKey.USER_STATE;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.AvroBuilder;
import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.user.UserAvroBuilder;
import andrewgrant.friendsdrinks.user.avro.UserId;
import andrewgrant.friendsdrinks.user.avro.UserState;


/**
 * Processes invitation requests.
 */
public class RequestService {

    private static final Logger log = LoggerFactory.getLogger(RequestService.class);

    private Properties envProps;
    private andrewgrant.friendsdrinks.AvroBuilder avroBuilder;
    private UserAvroBuilder userAvroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder;

    public RequestService(Properties envProps, AvroBuilder avroBuilder,
                          UserAvroBuilder userAvroBuilder,
                          andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
        this.userAvroBuilder = userAvroBuilder;
        this.frontendAvroBuilder = frontendAvroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, FriendsDrinksEvent> apiEvents = builder.stream(envProps.getProperty(FRIENDSDRINKS_API),
                Consumed.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty(FRIENDSDRINKS_STATE),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        KTable<andrewgrant.friendsdrinks.user.avro.UserId, UserState> userState =
                builder.table(envProps.getProperty(USER_STATE),
                        Consumed.with(userAvroBuilder.userIdSerde(), userAvroBuilder.userStateSerde()));

        String pendingInvitationsTopicName = envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_PENDING_INVITATION);
        KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations = builder.table(
                pendingInvitationsTopicName,
                Consumed.with(frontendAvroBuilder.friendsDrinksPendingInvitationIdSerde(),
                        frontendAvroBuilder.friendsDrinksPendingInvitationSerde()));

        handleInvitationRequests(apiEvents, friendsDrinksStateKTable, userState, pendingInvitationsTopicName);
        handleInvitationReplies(apiEvents, pendingFriendsDrinksInvitations, pendingInvitationsTopicName);

        KStream<String, FriendsDrinksRemoveUserRequest> removeUserRequests = apiEvents
                .filter((key, value) -> value.getEventType().equals(EventType.FRIENDSDRINKS_REMOVE_USER_REQUEST))
                .mapValues(value -> value.getFriendsDrinksRemoveUserRequest());

        handleUserRemovals(removeUserRequests, friendsDrinksStateKTable, userState);

        return builder.build();
    }

    private void handleUserRemovals(KStream<String, FriendsDrinksRemoveUserRequest> friendsDrinksRemoveUserRequest,
                                    KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksState,
                                    KTable<UserId, UserState> userState) {

        KStream<String, RemoveUserResult> resultsAfterValidatingUserState = friendsDrinksRemoveUserRequest
                .selectKey((key, value) -> UserId.newBuilder().setUserId(value.getUserIdToRemove().getUserId()).build())
                .leftJoin(userState,
                        (request, state) -> {
                            RemoveUserResult removeUserResult = new RemoveUserResult();
                            removeUserResult.friendsDrinksRemoveUserRequest = request;
                            if (state == null) {
                                log.warn(String.format("Failed to find user ID %s", request.getUserIdToRemove().getUserId()));
                            }
                            removeUserResult.failed = false;
                            return removeUserResult;
                        },
                        Joined.with(userAvroBuilder.userIdSerde(),
                                frontendAvroBuilder.friendsDrinksRemoveUserRequestSerde(),
                                userAvroBuilder.userStateSerde()))
                .selectKey((key, value) -> value.friendsDrinksRemoveUserRequest.getRequestId());

        KStream<String, RemoveUserResult>[] branchedResultsAfterValidatingUserState =
                resultsAfterValidatingUserState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> true)
                );

        convertToFailedRemoveUserResponse(branchedResultsAfterValidatingUserState[0]
                .mapValues(value -> value.friendsDrinksRemoveUserRequest))
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));

        KStream<String, RemoveUserResult> resultsAfterValidatingFriendsDrinksState =
                branchedResultsAfterValidatingUserState[1].selectKey((key, value) ->
                        andrewgrant.friendsdrinks.avro.FriendsDrinksId
                                .newBuilder()
                                .setUuid(value.friendsDrinksRemoveUserRequest.getFriendsDrinksId().getUuid())
                                .setAdminUserId(value.friendsDrinksRemoveUserRequest.getFriendsDrinksId().getAdminUserId())
                                .build())
                        .mapValues(value -> value.friendsDrinksRemoveUserRequest)
                        .leftJoin(friendsDrinksState,
                                (request, state) -> {
                                    RemoveUserResult removeUserResult = new RemoveUserResult();
                                    if (state != null) {
                                        if (request.getRequester().getUserId().equals(state.getFriendsDrinksId().getAdminUserId())) {
                                            removeUserResult.failed = false;
                                        } else {
                                            removeUserResult.failed = true;
                                        }
                                    } else {
                                        removeUserResult.failed = true;
                                    }
                                    return removeUserResult;
                                },
                                Joined.with(avroBuilder.friendsDrinksIdSerde(), frontendAvroBuilder.friendsDrinksRemoveUserRequestSerde(),
                                        avroBuilder.friendsDrinksStateSerde()))
                        .selectKey((key, value) -> value.friendsDrinksRemoveUserRequest.getRequestId());

        KStream<String, RemoveUserResult>[] branchedResultsAfterValidatingFriendsDrinksState =
                resultsAfterValidatingFriendsDrinksState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> true)
                );

        convertToFailedRemoveUserResponse(branchedResultsAfterValidatingFriendsDrinksState[0]
                .mapValues(value -> value.friendsDrinksRemoveUserRequest))
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));

        branchedResultsAfterValidatingFriendsDrinksState[1].mapValues(value -> {
            FriendsDrinksRemoveUserResponse removeUserResponse = FriendsDrinksRemoveUserResponse
                    .newBuilder()
                    .setRequestId(value.friendsDrinksRemoveUserRequest.getRequestId())
                    .setResult(Result.SUCCESS)
                    .build();
            return FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(EventType.FRIENDSDRINKS_REMOVE_USER_RESPONSE)
                    .setFriendsDrinksRemoveUserResponse(removeUserResponse)
                    .build();
        }).to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));
    }

    private KStream<String, FriendsDrinksEvent> convertToFailedRemoveUserResponse(KStream<String, FriendsDrinksRemoveUserRequest> requests) {
        return requests.mapValues(value -> {
            FriendsDrinksRemoveUserResponse removeUserResponse = FriendsDrinksRemoveUserResponse
                    .newBuilder()
                    .setRequestId(value.getRequestId())
                    .setResult(Result.FAIL)
                    .build();
            return FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(EventType.FRIENDSDRINKS_REMOVE_USER_RESPONSE)
                    .setFriendsDrinksRemoveUserResponse(removeUserResponse)
                    .build();
        });

    }

    private void handleInvitationRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                          KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                          KTable<UserId, UserState> userState, String pendingInvitationsTopicName) {

        // FriendsDrinks invitation requests
        KStream<String, FriendsDrinksInvitationRequest> friendsDrinksInvitations = apiEvents
                .filter((key, value) -> value.getEventType().equals(EventType.FRIENDSDRINKS_INVITATION_REQUEST))
                .mapValues(value -> value.getFriendsDrinksInvitationRequest());

        KStream<String, InvitationResult> resultsAfterValidatingFriendsDrinksState = friendsDrinksInvitations.selectKey((key, value) ->
                andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setUuid(value.getFriendsDrinksId().getUuid())
                        .build())
                .leftJoin(friendsDrinksStateKTable,
                        (request, state) -> {
                            InvitationResult invitationResult = new InvitationResult();
                            if (request.getUserId().getUserId().equals(request.getFriendsDrinksId().getAdminUserId())) {
                                invitationResult.failed = true;
                                return invitationResult;
                            }
                            // Validate request against state of FriendsDrinks
                            invitationResult.invitationRequest = request;
                            if (state != null) {
                                invitationResult.failed = false;
                            } else {
                                invitationResult.failed = true;
                            }
                            return invitationResult;
                        },
                        Joined.with(avroBuilder.friendsDrinksIdSerde(),
                                frontendAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                avroBuilder.friendsDrinksStateSerde())
                )
                .selectKey((key, value) -> value.invitationRequest.getRequestId());

        KStream<String, InvitationResult>[] branchedResultsAfterValidatingFriendsDrinksState =
                resultsAfterValidatingFriendsDrinksState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> true)
                );

        convertToFailedInvitationResponse(branchedResultsAfterValidatingFriendsDrinksState[0].mapValues(value -> value.invitationRequest))
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));

        KStream<String, InvitationResult> resultsAfterValidatingUserState = branchedResultsAfterValidatingFriendsDrinksState[1]
                .selectKey((key, value) -> UserId.newBuilder().setUserId(value.invitationRequest.getUserId().getUserId()).build())
                .mapValues(value -> value.invitationRequest)
                .leftJoin(userState,
                        (request, state) -> {
                            InvitationResult invitationResult = new InvitationResult();
                            invitationResult.invitationRequest = request;
                            if (state != null) {
                                invitationResult.failed = false;
                            } else {
                                invitationResult.failed = true;
                            }
                            return invitationResult;
                        },
                        Joined.with(userAvroBuilder.userIdSerde(), frontendAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                userAvroBuilder.userStateSerde()))
                .selectKey((key, value) -> value.invitationRequest.getRequestId());

        KStream<String, InvitationResult>[] branchedResultsAfterValidatingUserState =
                resultsAfterValidatingUserState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> true)
                );

        convertToFailedInvitationResponse(branchedResultsAfterValidatingUserState[0].mapValues(value -> value.invitationRequest))
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));

        KStream<String, FriendsDrinksInvitationRequest> acceptedInvitationRequests = branchedResultsAfterValidatingUserState[1]
                .mapValues(value -> value.invitationRequest);

        acceptedInvitationRequests.selectKey((key, value) ->
                andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setUuid(value.getFriendsDrinksId().getUuid())
                        .build())
                .leftJoin(friendsDrinksStateKTable,
                        (request, state) -> {
                            if (state != null) {
                                FriendsDrinksPendingInvitation pendingInvitation = FriendsDrinksPendingInvitation
                                        .newBuilder()
                                        .setFriendsDrinksId(request.getFriendsDrinksId())
                                        .setUserId(request.getUserId())
                                        .setInvitationId(
                                                FriendsDrinksPendingInvitationId
                                                        .newBuilder()
                                                        .setFriendsDrinksId(request.getFriendsDrinksId())
                                                        .setUserId(request.getUserId())
                                                        .build())
                                        .setMessage(String.format("Want to join %s?!", state.getName()))
                                        .build();
                                return pendingInvitation;
                            } else {
                                throw new RuntimeException(String.format("Failed to find FriendsDrinks state %s",
                                        request.getRequestId()));
                            }
                        },
                        Joined.with(avroBuilder.friendsDrinksIdSerde(),
                                frontendAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                avroBuilder.friendsDrinksStateSerde())
                )
                .selectKey((key, value) -> value.getInvitationId())
                .to(pendingInvitationsTopicName,
                        Produced.with(frontendAvroBuilder.friendsDrinksPendingInvitationIdSerde(),
                                frontendAvroBuilder.friendsDrinksPendingInvitationSerde()));

        acceptedInvitationRequests.map((key, value) -> {
            FriendsDrinksInvitationResponse response = FriendsDrinksInvitationResponse
                    .newBuilder()
                    .setRequestId(value.getRequestId())
                    .setResult(Result.SUCCESS)
                    .build();
            FriendsDrinksEvent friendsDrinksEvent = FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(EventType.FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setRequestId(value.getRequestId())
                    .setFriendsDrinksInvitationResponse(response)
                    .build();
            return new KeyValue<>(
                    friendsDrinksEvent.getFriendsDrinksInvitationResponse().getRequestId(),
                    friendsDrinksEvent);
        }).to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));
    }

    private KStream<String, FriendsDrinksEvent> convertToFailedInvitationResponse(KStream<String, FriendsDrinksInvitationRequest> stream) {
        return stream.mapValues(value -> {
            FriendsDrinksInvitationResponse response = FriendsDrinksInvitationResponse
                    .newBuilder()
                    .setResult(Result.FAIL)
                    .setRequestId(value.getRequestId())
                    .build();
            return FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(EventType.FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setFriendsDrinksInvitationResponse(response)
                    .setRequestId(response.getRequestId())
                    .build();
        });
    }

    private void handleInvitationReplies(KStream<String, FriendsDrinksEvent> apiEvents,
                                         KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations,
                                         String pendingInvitationsTopicName) {

        // FriendsDrinks replies
        KStream<String, FriendsDrinksInvitationReplyRequest> createFriendsDrinksInvitationReplyRequests = apiEvents
                .filter((key, value) -> value.getEventType().equals(EventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                .mapValues(value -> value.getFriendsDrinksInvitationReplyRequest());
        KStream<FriendsDrinksPendingInvitationId, FriendsDrinksInvitationReplyResponse> friendsDrinksInvitationReplyResponses =
                createFriendsDrinksInvitationReplyRequests.selectKey((key, value) -> FriendsDrinksPendingInvitationId
                        .newBuilder()
                        .setFriendsDrinksId(value.getFriendsDrinksId())
                        .setUserId(value.getUserId())
                        .build())
                        .leftJoin(pendingFriendsDrinksInvitations,
                                (request, state) -> {
                                    if (state != null) {
                                        return FriendsDrinksInvitationReplyResponse
                                                .newBuilder()
                                                .setRequestId(request.getRequestId())
                                                .setResult(Result.SUCCESS)
                                                .build();
                                    } else {
                                        log.info("Rejecting invitation reply for {}", request.getRequestId());
                                        return FriendsDrinksInvitationReplyResponse
                                                .newBuilder()
                                                .setRequestId(request.getRequestId())
                                                .setResult(Result.FAIL)
                                                .build();
                                    }
                                },
                                Joined.with(frontendAvroBuilder.friendsDrinksPendingInvitationIdSerde(),
                                        frontendAvroBuilder.friendsDrinksInvitationReplyRequestSerde(),
                                        frontendAvroBuilder.friendsDrinksPendingInvitationSerde())
                        );

        friendsDrinksInvitationReplyResponses.selectKey(((key, value) -> value.getRequestId()))
                .mapValues(value -> FriendsDrinksEvent
                        .newBuilder()
                        .setEventType(EventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE)
                        .setRequestId(value.getRequestId())
                        .setFriendsDrinksInvitationReplyResponse(value)
                        .build())
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.friendsDrinksSerde()));

        friendsDrinksInvitationReplyResponses.filter((key, value) -> value.getResult().equals(Result.SUCCESS))
                .mapValues(value -> (FriendsDrinksPendingInvitation) null)
                .to(pendingInvitationsTopicName,
                        Produced.with(
                                frontendAvroBuilder.friendsDrinksPendingInvitationIdSerde(),
                                frontendAvroBuilder.friendsDrinksPendingInvitationSerde()));
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-invitation-request.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        String registryUrl = envProps.getProperty("schema.registry.url");
        RequestService service = new RequestService(envProps, new AvroBuilder(registryUrl), new UserAvroBuilder(registryUrl),
                new andrewgrant.friendsdrinks.frontend.AvroBuilder(registryUrl));
        Topology topology = service.buildTopology();
        Properties streamProps = service.buildStreamProperties(envProps);
        KafkaStreams streams = new KafkaStreams(topology, streamProps);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
    }

}
