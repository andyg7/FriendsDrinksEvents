package andrewgrant.friendsdrinks.membership;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.AvroBuilder;
import andrewgrant.friendsdrinks.InvitationResult;
import andrewgrant.friendsdrinks.RemoveUserResult;
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

    public Topology buildTopology(Properties envProps, andrewgrant.friendsdrinks.AvroBuilder avroBuilder,
                                  UserAvroBuilder userAvroBuilder,
                                  andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder) {

        StreamsBuilder builder = new StreamsBuilder();

        final String apiTopicName = envProps.getProperty("friendsdrinks-api.topic.name");
        KStream<String, FriendsDrinksEvent> apiEvents = builder.stream(apiTopicName,
                Consumed.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty("friendsdrinks-state.topic.name"),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        KTable<andrewgrant.friendsdrinks.user.avro.UserId, UserState> userState =
                builder.table(envProps.getProperty("user-state.topic.name"),
                        Consumed.with(userAvroBuilder.userIdSerde(), userAvroBuilder.userStateSerde()));

        String pendingInvitationsTopicName = envProps.getProperty("friendsdrinks-pending-invitation.topic.name");
        KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations = builder.table(
                pendingInvitationsTopicName,
                Consumed.with(apiAvroBuilder.friendsDrinksPendingInvitationIdSerde(), apiAvroBuilder.friendsDrinksPendingInvitationSerde()));
        handleInvitations(apiEvents, friendsDrinksStateKTable, pendingFriendsDrinksInvitations, userState, avroBuilder,
                userAvroBuilder, apiAvroBuilder, apiTopicName, pendingInvitationsTopicName);

        handleUserRemovals(apiEvents, friendsDrinksStateKTable, userState, avroBuilder, userAvroBuilder, apiAvroBuilder,
                apiTopicName);

        return builder.build();
    }

    private void handleUserRemovals(KStream<String, FriendsDrinksEvent> apiEvents,
                                    KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                    KTable<UserId, UserState> userState,
                                    andrewgrant.friendsdrinks.AvroBuilder avroBuilder, UserAvroBuilder userAvroBuilder,
                                    andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder, String apiTopicName) {

        // FriendsDrinks invitation requests
        KStream<String, FriendsDrinksRemoveUserRequest> friendsDrinksRemoveUserRequest = apiEvents
                .filter((key, value) -> value.getEventType().equals(EventType.FRIENDSDRINKS_REMOVE_USER_REQUEST))
                .mapValues(value -> value.getFriendsDrinksRemoveUserRequest());

        KStream<String, RemoveUserResult> resultsAfterValidatingUserState = friendsDrinksRemoveUserRequest
                .selectKey((key, value) -> UserId.newBuilder().setUserId(value.getUserIdToRemove().getUserId()).build())
                .leftJoin(userState,
                        (request, state) -> {
                            RemoveUserResult removeUserResult = new RemoveUserResult();
                            removeUserResult.friendsDrinksRemoveUserRequest = request;
                            if (state != null) {
                                removeUserResult.failed = false;
                            } else {
                                removeUserResult.failed = true;
                            }
                            return removeUserResult;
                        },
                        Joined.with(userAvroBuilder.userIdSerde(),
                                apiAvroBuilder.friendsDrinksRemoveUserRequestSerde(),
                                userAvroBuilder.userStateSerde()))
                .selectKey((key, value) -> value.friendsDrinksRemoveUserRequest.getRequestId());

        KStream<String, RemoveUserResult>[] branchedResultsAfterValidatingUserState =
                resultsAfterValidatingUserState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> true)
                );

        emitFailedRemoveUserRequests(branchedResultsAfterValidatingUserState[0]
                .mapValues(value -> value.friendsDrinksRemoveUserRequest), apiTopicName, apiAvroBuilder);

        KStream<String, RemoveUserResult> resultsAfterValidatingFriendsDrinksState =
                branchedResultsAfterValidatingUserState[1].selectKey((key, value) ->
                        andrewgrant.friendsdrinks.avro.FriendsDrinksId
                                .newBuilder()
                                .setUuid(value.friendsDrinksRemoveUserRequest.getFriendsDrinksId().getUuid())
                                .setAdminUserId(value.friendsDrinksRemoveUserRequest.getFriendsDrinksId().getAdminUserId())
                                .build())
                        .mapValues(value -> value.friendsDrinksRemoveUserRequest)
                        .leftJoin(friendsDrinksStateKTable,
                                (request, state) -> {
                                    RemoveUserResult removeUserResult = new RemoveUserResult();
                                    if (state != null) {
                                        if (request.getRequester().getUserId().equals(state.getFriendsDrinksId().getAdminUserId())) {
                                            removeUserResult.failed = false;
                                        } else {
                                            if (state.getUserIds() != null &&
                                                    state.getUserIds().contains(request.getUserIdToRemove().getUserId())) {
                                                removeUserResult.failed = false;
                                            } else {
                                                removeUserResult.failed = true;
                                            }
                                        }
                                    } else {
                                        removeUserResult.failed = true;
                                    }
                                    return removeUserResult;
                                },
                                Joined.with(avroBuilder.friendsDrinksIdSerde(), apiAvroBuilder.friendsDrinksRemoveUserRequestSerde(),
                                        avroBuilder.friendsDrinksStateSerde()))
                        .selectKey((key, value) -> value.friendsDrinksRemoveUserRequest.getRequestId());

        KStream<String, RemoveUserResult>[] branchedResultsAfterValidatingFriendsDrinksState =
                resultsAfterValidatingFriendsDrinksState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> true)
                );

        emitFailedRemoveUserRequests(branchedResultsAfterValidatingFriendsDrinksState[0]
                .mapValues(value -> value.friendsDrinksRemoveUserRequest), apiTopicName, apiAvroBuilder);

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
        }).to(apiTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));
    }

    private void emitFailedRemoveUserRequests(KStream<String, FriendsDrinksRemoveUserRequest> requests,
                                              String apiTopicName,
                                              andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder) {
        requests.mapValues(value -> {
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
        }).to(apiTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));

    }

    private void handleInvitations(KStream<String, FriendsDrinksEvent> apiEvents,
                                   KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                   KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations,
                                   KTable<andrewgrant.friendsdrinks.user.avro.UserId, UserState> userState,
                                   andrewgrant.friendsdrinks.AvroBuilder avroBuilder, UserAvroBuilder userAvroBuilder,
                                   andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder, String apiTopicName,
                                   String pendingInvitationsTopicName) {

        handleInvitationRequests(apiEvents, friendsDrinksStateKTable, userState, avroBuilder, apiAvroBuilder, userAvroBuilder, apiTopicName,
                pendingInvitationsTopicName);
        handleInvitationReplies(apiEvents, pendingFriendsDrinksInvitations, apiAvroBuilder, apiTopicName,
                pendingInvitationsTopicName);

    }

    private void handleInvitationRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                          KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                          KTable<UserId, UserState> userState, andrewgrant.friendsdrinks.AvroBuilder avroBuilder,
                                          andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                          UserAvroBuilder userAvroBuilder, String apiTopicName, String pendingInvitationsTopicName) {

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
                            // Validate request against state of FriendsDrinks
                            InvitationResult invitationResult = new InvitationResult();
                            invitationResult.invitationRequest = request;
                            if (state != null) {
                                if (state.getUserIds() != null &&
                                        state.getUserIds().contains(request.getUserId()) ||
                                        state.getFriendsDrinksId().getAdminUserId().equals(request.getUserId().getUserId())) {
                                    invitationResult.failed = true;
                                } else {
                                    // Confirms the FriendsDrinks exists.
                                    invitationResult.failed = false;
                                }
                            } else {
                                invitationResult.failed = true;
                            }
                            return invitationResult;
                        },
                        Joined.with(avroBuilder.friendsDrinksIdSerde(),
                                apiAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                avroBuilder.friendsDrinksStateSerde())
                )
                .selectKey((key, value) -> value.invitationRequest.getRequestId());

        KStream<String, InvitationResult>[] branchedResultsAfterValidatingFriendsDrinksState =
                resultsAfterValidatingFriendsDrinksState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> !value.failed)
                );

        emitFailedInvitationRequests(branchedResultsAfterValidatingFriendsDrinksState[0].mapValues(value -> value.invitationRequest),
                apiAvroBuilder, apiTopicName);

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
                        Joined.with(userAvroBuilder.userIdSerde(), apiAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                userAvroBuilder.userStateSerde()))
                .selectKey((key, value) -> value.invitationRequest.getRequestId());

        KStream<String, InvitationResult>[] branchedResultsAfterValidatingUserState =
                resultsAfterValidatingUserState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> !value.failed)
                );

        emitFailedInvitationRequests(branchedResultsAfterValidatingUserState[0].mapValues(value -> value.invitationRequest),
                apiAvroBuilder, apiTopicName);

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
                                apiAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                avroBuilder.friendsDrinksStateSerde())
                )
                .selectKey((key, value) -> value.getInvitationId())
                .to(pendingInvitationsTopicName,
                        Produced.with(apiAvroBuilder.friendsDrinksPendingInvitationIdSerde(),
                                apiAvroBuilder.friendsDrinksPendingInvitationSerde()));

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
        }).to(apiTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));
    }

    private void emitFailedInvitationRequests(KStream<String, FriendsDrinksInvitationRequest> stream,
                                              andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                              String apiTopicName) {
        stream.mapValues(value -> {
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
        }).to(apiTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));
    }

    private void handleInvitationReplies(KStream<String, FriendsDrinksEvent> apiEvents,
                                         KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations,
                                         andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                         String apiTopicName, String pendingInvitationsTopicName) {

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
                                Joined.with(apiAvroBuilder.friendsDrinksPendingInvitationIdSerde(),
                                        apiAvroBuilder.friendsDrinksInvitationReplyRequestSerde(),
                                        apiAvroBuilder.friendsDrinksPendingInvitationSerde())
                        );

        friendsDrinksInvitationReplyResponses.selectKey(((key, value) -> value.getRequestId()))
                .mapValues(value -> FriendsDrinksEvent
                        .newBuilder()
                        .setEventType(EventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE)
                        .setRequestId(value.getRequestId())
                        .setFriendsDrinksInvitationReplyResponse(value)
                        .build())
                .to(apiTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));

        friendsDrinksInvitationReplyResponses.filter((key, value) -> value.getResult().equals(Result.SUCCESS))
                .mapValues(value -> (FriendsDrinksPendingInvitation) null)
                .to(pendingInvitationsTopicName,
                        Produced.with(apiAvroBuilder.friendsDrinksPendingInvitationIdSerde(), apiAvroBuilder.friendsDrinksPendingInvitationSerde()));
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-invitation-request.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        RequestService service = new RequestService();
        String registryUrl = envProps.getProperty("schema.registry.url");
        Topology topology = service.buildTopology(envProps, new AvroBuilder(registryUrl), new UserAvroBuilder(registryUrl),
                new andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder(registryUrl));
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
