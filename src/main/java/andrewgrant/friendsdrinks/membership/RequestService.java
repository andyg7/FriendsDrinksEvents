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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksInvitation;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksInvitationId;
import andrewgrant.friendsdrinks.user.AvroBuilder;
import andrewgrant.friendsdrinks.user.avro.UserId;
import andrewgrant.friendsdrinks.user.avro.UserState;


/**
 * Processes invitation requests.
 */
public class RequestService {

    private static final Logger log = LoggerFactory.getLogger(RequestService.class);

    private Properties envProps;
    private andrewgrant.friendsdrinks.AvroBuilder avroBuilder;
    private AvroBuilder userAvroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder;
    private andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder;

    public RequestService(Properties envProps, andrewgrant.friendsdrinks.AvroBuilder avroBuilder,
                          AvroBuilder userAvroBuilder, andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder,
                          andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
        this.userAvroBuilder = userAvroBuilder;
        this.frontendAvroBuilder = frontendAvroBuilder;
        this.membershipAvroBuilder = membershipAvroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ApiEvent> apiEvents = builder.stream(envProps.getProperty(FRIENDSDRINKS_API),
                Consumed.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty(FRIENDSDRINKS_STATE),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        KTable<andrewgrant.friendsdrinks.user.avro.UserId, UserState> userState =
                builder.table(envProps.getProperty(USER_STATE),
                        Consumed.with(userAvroBuilder.userIdSerde(), userAvroBuilder.userStateSerde()));

        KTable<FriendsDrinksInvitationId, FriendsDrinksInvitation> friendsDrinksInvitations =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION),
                        Consumed.with(membershipAvroBuilder.friendsDrinksInvitationIdSerde(),
                                membershipAvroBuilder.friendsDrinksInvitationSerde()));

        // FriendsDrinks invitation requests
        KStream<String, FriendsDrinksInvitationRequest> friendsDrinksInvitationRequests = apiEvents
                .filter((key, value) -> value.getEventType().equals(ApiEventType.FRIENDSDRINKS_INVITATION_REQUEST))
                .mapValues(value -> value.getFriendsDrinksInvitationRequest());
        InvitationRequestResult invitationRequestResult =
                handleInvitationRequests(friendsDrinksInvitationRequests, friendsDrinksStateKTable, userState);

        invitationRequestResult.getSuccessfulResponseKStream()
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));
        for (KStream<String, ApiEvent> friendsDrinksEventKStream : invitationRequestResult.getFailedResponseKStreams()) {
            friendsDrinksEventKStream
                    .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));
        }

        // FriendsDrinks replies
        KStream<String, FriendsDrinksInvitationReplyRequest> friendsDrinksInvitationReplyRequests = apiEvents
                .filter((key, value) -> value.getEventType().equals(ApiEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                .mapValues(value -> value.getFriendsDrinksInvitationReplyRequest());
        handleInvitationReplies(friendsDrinksInvitationReplyRequests,  friendsDrinksInvitations)
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        KStream<String, FriendsDrinksRemoveUserRequest> removeUserRequests = apiEvents.filter((key, value) ->
                value.getEventType().equals(ApiEventType.FRIENDSDRINKS_REMOVE_USER_REQUEST))
                .mapValues(value -> value.getFriendsDrinksRemoveUserRequest());

        RemoveUserRequestResult removeUserRequestResult = handleRemoveUserRequests(removeUserRequests, friendsDrinksStateKTable, userState);
        removeUserRequestResult.getSuccessfulResponseKStream()
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));
        for (KStream<String, ApiEvent> friendsDrinksEventKStream : removeUserRequestResult.getFailedResponseKStreams()) {
            friendsDrinksEventKStream
                    .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));
        }

        return builder.build();
    }

    private RemoveUserRequestResult handleRemoveUserRequests(
            KStream<String, FriendsDrinksRemoveUserRequest> friendsDrinksRemoveUserRequest,
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

        RemoveUserRequestResult result = new RemoveUserRequestResult();
        result.addFailedResponse(
                convertToFailedRemoveUserResponse(
                        branchedResultsAfterValidatingUserState[0].mapValues(value -> value.friendsDrinksRemoveUserRequest)));

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

        result.addFailedResponse(
                convertToFailedRemoveUserResponse(
                        branchedResultsAfterValidatingFriendsDrinksState[0].mapValues(value -> value.friendsDrinksRemoveUserRequest)));

        result.setSuccessfulResponseKStream(branchedResultsAfterValidatingFriendsDrinksState[1].mapValues(value -> {
            FriendsDrinksRemoveUserResponse removeUserResponse = FriendsDrinksRemoveUserResponse
                    .newBuilder()
                    .setRequestId(value.friendsDrinksRemoveUserRequest.getRequestId())
                    .setResult(Result.SUCCESS)
                    .build();
            return ApiEvent
                    .newBuilder()
                    .setEventType(ApiEventType.FRIENDSDRINKS_REMOVE_USER_RESPONSE)
                    .setFriendsDrinksRemoveUserResponse(removeUserResponse)
                    .build();
        }));
        return result;
    }

    private KStream<String, ApiEvent> convertToFailedRemoveUserResponse(KStream<String, FriendsDrinksRemoveUserRequest> requests) {
        return requests.mapValues(value -> {
            FriendsDrinksRemoveUserResponse removeUserResponse = FriendsDrinksRemoveUserResponse
                    .newBuilder()
                    .setRequestId(value.getRequestId())
                    .setResult(Result.FAIL)
                    .build();
            return ApiEvent
                    .newBuilder()
                    .setEventType(ApiEventType.FRIENDSDRINKS_REMOVE_USER_RESPONSE)
                    .setFriendsDrinksRemoveUserResponse(removeUserResponse)
                    .build();
        });

    }

    private InvitationRequestResult handleInvitationRequests(KStream<String, FriendsDrinksInvitationRequest> friendsDrinksInvitations ,
                                          KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                          KTable<UserId, UserState> userState) {

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


        InvitationRequestResult result = new InvitationRequestResult();
        result.addFailedResponse(
                convertToFailedInvitationResponse(branchedResultsAfterValidatingFriendsDrinksState[0].mapValues(value -> value.invitationRequest)));

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

        result.addFailedResponse(
                convertToFailedInvitationResponse(branchedResultsAfterValidatingUserState[0].mapValues(value -> value.invitationRequest)));

        KStream<String, FriendsDrinksInvitationRequest> acceptedInvitationRequests = branchedResultsAfterValidatingUserState[1]
                .mapValues(value -> value.invitationRequest);

        result.setSuccessfulResponseKStream(acceptedInvitationRequests.map((key, value) -> {
            FriendsDrinksInvitationResponse response = FriendsDrinksInvitationResponse
                    .newBuilder()
                    .setRequestId(value.getRequestId())
                    .setResult(Result.SUCCESS)
                    .build();
            ApiEvent friendsDrinksEvent = ApiEvent
                    .newBuilder()
                    .setEventType(ApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setRequestId(value.getRequestId())
                    .setFriendsDrinksInvitationResponse(response)
                    .build();
            return new KeyValue<>(
                    friendsDrinksEvent.getFriendsDrinksInvitationResponse().getRequestId(),
                    friendsDrinksEvent);
        }));

        return result;
    }

    private static class InvitationRequestResult {

        private List<KStream<String, ApiEvent>> failedResponseKStreams;
        private KStream<String, ApiEvent> successfulResponseKStream;

        public InvitationRequestResult() {
            this.failedResponseKStreams = new ArrayList<>();
            this.successfulResponseKStream = null;
        }

        public List<KStream<String, ApiEvent>> getFailedResponseKStreams() {
            return failedResponseKStreams;
        }

        public KStream<String, ApiEvent> getSuccessfulResponseKStream() {
            return successfulResponseKStream;
        }

        public void addFailedResponse(KStream<String, ApiEvent> failedResponse) {
            failedResponseKStreams.add(failedResponse);
        }

        public void setSuccessfulResponseKStream(KStream<String, ApiEvent> successfulResponseKStream) {
            this.successfulResponseKStream = successfulResponseKStream;
        }
    }

    private static class RemoveUserRequestResult {

        private List<KStream<String, ApiEvent>> failedResponseKStreams;
        private KStream<String, ApiEvent> successfulResponseKStream;

        public RemoveUserRequestResult() {
            this.failedResponseKStreams = new ArrayList<>();
            this.successfulResponseKStream = null;
        }

        public List<KStream<String, ApiEvent>> getFailedResponseKStreams() {
            return failedResponseKStreams;
        }

        public KStream<String, ApiEvent> getSuccessfulResponseKStream() {
            return successfulResponseKStream;
        }

        public void addFailedResponse(KStream<String, ApiEvent> failedResponse) {
            failedResponseKStreams.add(failedResponse);
        }

        public void setSuccessfulResponseKStream(KStream<String, ApiEvent> successfulResponseKStream) {
            this.successfulResponseKStream = successfulResponseKStream;
        }
    }

    private KStream<String, ApiEvent> convertToFailedInvitationResponse(KStream<String, FriendsDrinksInvitationRequest> stream) {
        return stream.mapValues(value -> {
            FriendsDrinksInvitationResponse response = FriendsDrinksInvitationResponse
                    .newBuilder()
                    .setResult(Result.FAIL)
                    .setRequestId(value.getRequestId())
                    .build();
            return ApiEvent
                    .newBuilder()
                    .setEventType(ApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setFriendsDrinksInvitationResponse(response)
                    .setRequestId(response.getRequestId())
                    .build();
        });
    }

    private KStream<String, ApiEvent> handleInvitationReplies(
            KStream<String, FriendsDrinksInvitationReplyRequest> invitationReplyRequestKStream,
            KTable<FriendsDrinksInvitationId, FriendsDrinksInvitation> friendsDrinksInvitations) {
        KStream<FriendsDrinksInvitationId, FriendsDrinksInvitationReplyResponse> friendsDrinksInvitationReplyResponses =
                invitationReplyRequestKStream.selectKey((key, value) -> FriendsDrinksInvitationId
                        .newBuilder()
                        .setFriendsDrinksId(andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId
                                .newBuilder()
                                .setUuid(value.getFriendsDrinksId().getUuid())
                                .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                                .build())
                        .setUserId(andrewgrant.friendsdrinks.membership.avro.UserId
                                .newBuilder()
                                .setUserId(value.getUserId().getUserId())
                                .build())
                        .build())
                        .leftJoin(friendsDrinksInvitations,
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
                                Joined.with(membershipAvroBuilder.friendsDrinksInvitationIdSerde(),
                                        frontendAvroBuilder.friendsDrinksInvitationReplyRequestSerde(),
                                        membershipAvroBuilder.friendsDrinksInvitationSerde())
                        );

        return friendsDrinksInvitationReplyResponses.selectKey(((key, value) -> value.getRequestId()))
                .mapValues(value -> ApiEvent
                        .newBuilder()
                        .setEventType(ApiEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE)
                        .setRequestId(value.getRequestId())
                        .setFriendsDrinksInvitationReplyResponse(value)
                        .build());
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
        RequestService service = new RequestService(envProps, new andrewgrant.friendsdrinks.AvroBuilder(registryUrl), new AvroBuilder(registryUrl),
                new andrewgrant.friendsdrinks.frontend.AvroBuilder(registryUrl),
                new andrewgrant.friendsdrinks.membership.AvroBuilder(registryUrl));
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
