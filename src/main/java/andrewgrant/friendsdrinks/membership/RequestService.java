package andrewgrant.friendsdrinks.membership;

import static andrewgrant.friendsdrinks.TopicNameConfigKey.FRIENDSDRINKS_STATE;
import static andrewgrant.friendsdrinks.env.Properties.load;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;
import static andrewgrant.friendsdrinks.user.TopicNameConfigKey.USER_STATE;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.*;

import com.sun.net.httpserver.HttpServer;

/**
 * Processes invitation requests.
 */
public class RequestService {

    private static final Logger log = LoggerFactory.getLogger(RequestService.class);
    private static final String PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE =
            "pending-friendsdrinks-membership-requests-store";

    private Properties envProps;
    private andrewgrant.friendsdrinks.AvroBuilder friendsDrinksAvroBuilder;
    private andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder;
    private andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder;

    public RequestService(Properties envProps,
                          andrewgrant.friendsdrinks.AvroBuilder friendsDrinksAvroBuilder,
                          andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder,
                          andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder,
                          andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder) {
        this.envProps = envProps;
        this.friendsDrinksAvroBuilder = friendsDrinksAvroBuilder;
        this.userAvroBuilder = userAvroBuilder;
        this.frontendAvroBuilder = frontendAvroBuilder;
        this.membershipAvroBuilder = membershipAvroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ApiEvent> apiEvents = builder.stream(envProps.getProperty(FRIENDSDRINKS_API),
                Consumed.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty(FRIENDSDRINKS_STATE),
                        Consumed.with(
                                friendsDrinksAvroBuilder.friendsDrinksIdSerde(),
                                friendsDrinksAvroBuilder.friendsDrinksStateSerde()));

        KTable<UserId, UserState> userState = builder.table(envProps.getProperty(USER_STATE),
                Consumed.with(userAvroBuilder.userIdSerde(), userAvroBuilder.userStateSerde()));

        KTable<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> friendsDrinksInvitations =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION_EVENT),
                        Consumed.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                                membershipAvroBuilder.friendsDrinksInvitationEventSerde()));

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE),
                membershipAvroBuilder.friendsDrinksMembershipIdSerdes(), Serdes.String());
        builder.addStateStore(storeBuilder);

        purgePendingRequestsStore(friendsDrinksInvitations.toStream());

        KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> friendsDrinksMembershipEventKStream =
                apiEvents.filter((key, value) -> value.getEventType().equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT))
                        .mapValues(x -> x.getFriendsDrinksMembershipEvent())
                        .filter((key, value) -> {
                            FriendsDrinksMembershipApiEventType eventType = value.getEventType();
                            return eventType.equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST) ||
                                    eventType.equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REQUEST);
                        })
                        .selectKey((key, value) -> FriendsDrinksMembershipId
                                .newBuilder()
                                .setFriendsDrinksId(value.getMembershipId().getFriendsDrinksId())
                                .setUserId(value.getMembershipId().getUserId())
                                .build());

        KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEventConcurrencyCheck>
                friendsDrinksMembershipEventConcurrencyCheckKStream = checkForConcurrentRequests(friendsDrinksMembershipEventKStream);

        KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEventConcurrencyCheck>[] branchedConcurrencyCheck =
                friendsDrinksMembershipEventConcurrencyCheckKStream.branch(
                        (key, value) -> value.isConcurrentRequest,
                        (key, value) -> true
                );

        toRejectedResponse(branchedConcurrencyCheck[0].mapValues(v -> v.friendsDrinksMembershipEvent))
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));

        InvitationRequestResult invitationRequestResult =
                handleInvitationRequests(branchedConcurrencyCheck[1]
                        .mapValues(v -> v.friendsDrinksMembershipEvent)
                        .filter((key, value) ->
                                value.getEventType().equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REQUEST))
                        .mapValues(v -> v.getFriendsDrinksInvitationRequest()), friendsDrinksStateKTable, userState);

        toApiResponse(invitationRequestResult.getSuccessfulResponseKStream())
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));

        for (KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> friendsDrinksEventKStream :
                invitationRequestResult.getFailedResponseKStreams()) {
            toApiResponse(friendsDrinksEventKStream)
                    .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));
        }

        toApiResponse(handleInvitationReplies(branchedConcurrencyCheck[1]
                .mapValues(v -> v.friendsDrinksMembershipEvent)
                .filter((key, value) ->
                        value.getEventType().equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                .mapValues(v -> v.getFriendsDrinksInvitationReplyRequest()),  friendsDrinksInvitations))
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));

        return builder.build();
    }

    private void purgePendingRequestsStore(
            KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent>
                    friendsDrinksInvitationKStream) {

        friendsDrinksInvitationKStream.selectKey((key, value) -> key)
                .repartition(Repartitioned.with(
                        membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                        membershipAvroBuilder.friendsDrinksInvitationEventSerde()))
                .process(() ->
                        new Processor<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent>() {

                            private KeyValueStore<FriendsDrinksMembershipId, String> stateStore;

                            @Override
                            public void init(ProcessorContext processorContext) {
                                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
                            }

                            @Override
                            public void process(FriendsDrinksMembershipId friendsDrinksMembershipId,
                                                FriendsDrinksInvitationEvent friendsDrinksInvitation) {
                                String requestId = stateStore.get(friendsDrinksMembershipId);
                                if (requestId != null && requestId.equals(friendsDrinksInvitation.getRequestId())) {
                                    stateStore.delete(friendsDrinksMembershipId);
                                } else {
                                    log.error("Failed to get request {} for FriendsDrinks UUID {}",
                                            requestId,
                                            friendsDrinksMembershipId.getFriendsDrinksId().getUuid());
                                }
                            }

                            @Override
                            public void close() { }
                        }, PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
    }

    private KStream<String, ApiEvent> toRejectedResponse(
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> friendsDrinksMembershipEventKStream) {
        return friendsDrinksMembershipEventKStream.map((key, value) -> {
            FriendsDrinksMembershipApiEventType eventType = value.getEventType();
            ApiEvent.Builder apiEventBuilder = ApiEvent
                    .newBuilder()
                    .setRequestId(value.getRequestId())
                    .setEventType(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT);
            String requestId = value.getRequestId();
            switch (eventType) {
                case FRIENDSDRINKS_INVITATION_REQUEST:
                    apiEventBuilder.setFriendsDrinksMembershipEvent(
                            FriendsDrinksMembershipApiEvent
                                    .newBuilder()
                                    .setRequestId(requestId)
                                    .setEventType(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE)
                                    .setMembershipId(value.getMembershipId())
                                    .setFriendsDrinksInvitationResponse(FriendsDrinksInvitationResponse
                                            .newBuilder()
                                            .setRequestId(requestId)
                                            .setResult(Result.FAIL)
                                            .build())
                                    .build());
                    break;
                case FRIENDSDRINKS_INVITATION_REPLY_REQUEST:
                    apiEventBuilder.setFriendsDrinksMembershipEvent(
                            FriendsDrinksMembershipApiEvent
                                    .newBuilder()
                                    .setEventType(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE)
                                    .setMembershipId(value.getMembershipId())
                                    .setRequestId(requestId)
                                    .setFriendsDrinksInvitationReplyResponse(FriendsDrinksInvitationReplyResponse
                                            .newBuilder()
                                            .setRequestId(requestId)
                                            .setResult(Result.FAIL)
                                            .build())
                                    .build());
                    break;
                default:
                    throw new RuntimeException(String.format("Unexpected event type %s", eventType.name()));
            }
            ApiEvent apiEvent = apiEventBuilder.build();
            return KeyValue.pair(apiEvent.getRequestId(), apiEvent);
        });
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEventConcurrencyCheck> checkForConcurrentRequests(
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> friendsDrinksMembershipEventKStream
    ) {
        return friendsDrinksMembershipEventKStream
                .repartition(Repartitioned.with(
                        membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                        frontendAvroBuilder.friendsDrinksMembershipApiEventSerde()))
                .transformValues(() ->
                        new ValueTransformer<FriendsDrinksMembershipApiEvent, FriendsDrinksMembershipEventConcurrencyCheck>() {

                            private KeyValueStore<FriendsDrinksMembershipId, String> stateStore;

                            @Override
                            public void init(ProcessorContext processorContext) {
                                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
                            }

                            @Override
                            public FriendsDrinksMembershipEventConcurrencyCheck transform(
                                    FriendsDrinksMembershipApiEvent friendsDrinksMembershipEvent) {
                                FriendsDrinksMembershipEventConcurrencyCheck concurrencyCheck =
                                        new FriendsDrinksMembershipEventConcurrencyCheck();
                                concurrencyCheck.friendsDrinksMembershipEvent = friendsDrinksMembershipEvent;
                                FriendsDrinksMembershipId friendsDrinksMembershipId = friendsDrinksMembershipEvent.getMembershipId();
                                if (stateStore.get(friendsDrinksMembershipId) != null) {
                                    concurrencyCheck.isConcurrentRequest = true;
                                    log.info("Found a concurrent request for FriendsDrinks UUID {} and User ID {} with request ID {}",
                                            friendsDrinksMembershipId.getFriendsDrinksId().getUuid(),
                                            friendsDrinksMembershipId.getUserId().getUserId(),
                                            friendsDrinksMembershipEvent.getRequestId());
                                } else {
                                    stateStore.put(friendsDrinksMembershipId, friendsDrinksMembershipEvent.getRequestId());
                                    concurrencyCheck.isConcurrentRequest = false;
                                }
                                return concurrencyCheck;
                            }

                            @Override
                            public void close() {

                            }
                        }, PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
    }

    private KStream<String, ApiEvent> toApiResponse(
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> friendsDrinksMembershipEventKStream) {

        return friendsDrinksMembershipEventKStream
                .repartition(Repartitioned.with(
                        membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                        frontendAvroBuilder.friendsDrinksMembershipApiEventSerde()))
                .transform(() ->
                        new Transformer<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent, KeyValue<String, ApiEvent>>() {

                            private KeyValueStore<FriendsDrinksMembershipId, String> stateStore;

                            @Override
                            public void init(ProcessorContext processorContext) {
                                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
                            }

                            @Override
                            public KeyValue<String, ApiEvent> transform(
                                    FriendsDrinksMembershipId friendsDrinksMembershipId,
                                    FriendsDrinksMembershipApiEvent friendsDrinksMembershipEvent) {
                                Result result;
                                FriendsDrinksMembershipApiEventType eventType = friendsDrinksMembershipEvent.getEventType();
                        switch (eventType) {
                            case FRIENDSDRINKS_INVITATION_RESPONSE:
                                result = friendsDrinksMembershipEvent.getFriendsDrinksInvitationResponse().getResult();
                                break;
                            case FRIENDSDRINKS_INVITATION_REPLY_RESPONSE:
                                result = friendsDrinksMembershipEvent.getFriendsDrinksInvitationReplyResponse().getResult();
                                break;
                            default:
                                throw new RuntimeException(String.format("Unexpected event type %s", eventType.name()));
                        }
                        if (result.equals(Result.FAIL)) {
                            String requestId = stateStore.get(friendsDrinksMembershipId);
                            if (requestId != null && requestId.equals(friendsDrinksMembershipEvent.getRequestId())) {
                                stateStore.delete(friendsDrinksMembershipId);
                            } else {
                                log.error("Failed to get request {} for FriendsDrinks UUID {}",
                                        requestId, friendsDrinksMembershipId.getFriendsDrinksId().getUuid());
                            }
                        }
                        ApiEvent apiEvent = ApiEvent
                                .newBuilder()
                                .setRequestId(friendsDrinksMembershipEvent.getRequestId())
                                .setEventType(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT)
                                .setFriendsDrinksMembershipEvent(friendsDrinksMembershipEvent)
                                .build();
                        return KeyValue.pair(apiEvent.getRequestId(), apiEvent);
                    }

                    @Override
                    public void close() {

                    }
                }, PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
    }

    private InvitationRequestResult handleInvitationRequests(
            KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationRequest> friendsDrinksInvitations ,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
            KTable<UserId, UserState> userState) {

        KStream<FriendsDrinksMembershipId, InvitationResult> resultsAfterValidatingFriendsDrinksState =
                friendsDrinksInvitations.selectKey((key, value) ->
                        FriendsDrinksId
                                .newBuilder()
                                .setUuid(value.getMembershipId().getFriendsDrinksId().getUuid())
                                .build())
                        .leftJoin(friendsDrinksStateKTable,
                                (request, state) -> {
                                    InvitationResult invitationResult = new InvitationResult();
                                    invitationResult.invitationRequest = request;
                                    // Validate request against state of FriendsDrinks
                                    if (state != null && (!state.getStatus().equals(FriendsDrinksStatus.DELETED))) {
                                        invitationResult.failed = false;
                                    } else {
                                        invitationResult.failed = true;
                                    }
                                    return invitationResult;
                                },
                                Joined.with(friendsDrinksAvroBuilder.friendsDrinksIdSerde(),
                                        frontendAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                        friendsDrinksAvroBuilder.friendsDrinksStateSerde())
                        )
                        .selectKey((key, value) -> value.invitationRequest.getMembershipId());

        KStream<FriendsDrinksMembershipId, InvitationResult>[] branchedResultsAfterValidatingFriendsDrinksState =
                resultsAfterValidatingFriendsDrinksState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> true)
                );


        InvitationRequestResult result = new InvitationRequestResult();
        result.addFailedResponse(
                convertToFailedInvitationResponse(branchedResultsAfterValidatingFriendsDrinksState[0].mapValues(value -> value.invitationRequest)));

        KStream<FriendsDrinksMembershipId, InvitationResult> resultsAfterValidatingUserState = branchedResultsAfterValidatingFriendsDrinksState[1]
                .selectKey((key, value) -> UserId
                        .newBuilder()
                        .setUserId(value.invitationRequest.getMembershipId().getUserId().getUserId())
                        .build())
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
                        Joined.with(
                                userAvroBuilder.userIdSerde(),
                                frontendAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                userAvroBuilder.userStateSerde()))
                .selectKey((key, value) -> value.invitationRequest.getMembershipId());

        KStream<FriendsDrinksMembershipId, InvitationResult>[] branchedResultsAfterValidatingUserState =
                resultsAfterValidatingUserState.branch(
                        ((key, value) -> value.failed),
                        ((key, value) -> true)
                );

        result.addFailedResponse(
                convertToFailedInvitationResponse(branchedResultsAfterValidatingUserState[0].mapValues(value -> value.invitationRequest)));

        KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationRequest> acceptedInvitationRequests = branchedResultsAfterValidatingUserState[1]
                .mapValues(value -> value.invitationRequest);

        result.setSuccessfulResponseKStream(acceptedInvitationRequests.map((key, value) -> {
            FriendsDrinksInvitationResponse response = FriendsDrinksInvitationResponse
                    .newBuilder()
                    .setRequestId(value.getRequestId())
                    .setResult(Result.SUCCESS)
                    .build();

            FriendsDrinksMembershipApiEvent friendsDrinksMembershipEvent = FriendsDrinksMembershipApiEvent.newBuilder()
                    .setEventType(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setRequestId(value.getRequestId())
                    .setFriendsDrinksInvitationResponse(response)
                    .setMembershipId(value.getMembershipId())
                    .build();
            return new KeyValue<>(
                    friendsDrinksMembershipEvent.getMembershipId(),
                    friendsDrinksMembershipEvent);
        }));

        return result;
    }

    private static class InvitationRequestResult {

        private List<KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent>> failedResponseKStreams;
        private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> successfulResponseKStream;

        public InvitationRequestResult() {
            this.failedResponseKStreams = new ArrayList<>();
            this.successfulResponseKStream = null;
        }

        public List<KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent>> getFailedResponseKStreams() {
            return failedResponseKStreams;
        }

        public KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> getSuccessfulResponseKStream() {
            return successfulResponseKStream;
        }

        public void addFailedResponse(KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> failedResponse) {
            failedResponseKStreams.add(failedResponse);
        }

        public void setSuccessfulResponseKStream(KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> successfulResponseKStream) {
            this.successfulResponseKStream = successfulResponseKStream;
        }
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> convertToFailedInvitationResponse(
            KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationRequest> stream) {
        return stream.map((key, value) -> {
            FriendsDrinksInvitationResponse response = FriendsDrinksInvitationResponse
                    .newBuilder()
                    .setResult(Result.FAIL)
                    .setRequestId(value.getRequestId())
                    .build();

            FriendsDrinksMembershipApiEvent friendsDrinksMembershipEvent = FriendsDrinksMembershipApiEvent.newBuilder()
                    .setEventType(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setFriendsDrinksInvitationResponse(response)
                    .setRequestId(response.getRequestId())
                    .setMembershipId(value.getMembershipId())
                    .setMembershipId(value.getMembershipId())
                    .build();

            return KeyValue.pair(friendsDrinksMembershipEvent.getMembershipId(), friendsDrinksMembershipEvent);
        });
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent> handleInvitationReplies(
            KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationReplyRequest> invitationReplyRequestKStream,
            KTable<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> friendsDrinksInvitations) {
        KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipApiEvent>
                friendsDrinksInvitationReplyResponses =
                invitationReplyRequestKStream.selectKey((key, value) -> FriendsDrinksMembershipId
                        .newBuilder()
                        .setFriendsDrinksId(value.getMembershipId().getFriendsDrinksId())
                        .setUserId(value.getMembershipId().getUserId())
                        .build())
                        .leftJoin(friendsDrinksInvitations,
                                (request, state) -> {
                                    FriendsDrinksMembershipApiEvent.Builder friendsDrinksMembershipEvent =
                                            FriendsDrinksMembershipApiEvent.newBuilder()
                                                    .setRequestId(request.getRequestId())
                                                    .setMembershipId(request.getMembershipId())
                                                    .setEventType(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE);
                                    if (state != null) {
                                        friendsDrinksMembershipEvent.setFriendsDrinksInvitationReplyResponse(FriendsDrinksInvitationReplyResponse
                                                .newBuilder()
                                                .setRequestId(request.getRequestId())
                                                .setResult(Result.SUCCESS)
                                                .build());
                                    } else {
                                        log.info("Rejecting invitation reply for {}", request.getRequestId());
                                        friendsDrinksMembershipEvent.setFriendsDrinksInvitationReplyResponse(
                                                FriendsDrinksInvitationReplyResponse
                                                        .newBuilder()
                                                        .setRequestId(request.getRequestId())
                                                        .setResult(Result.FAIL)
                                                        .build());
                                    }
                                    return friendsDrinksMembershipEvent.build();
                                },
                                Joined.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                                        frontendAvroBuilder.friendsDrinksInvitationReplyRequestSerde(),
                                        membershipAvroBuilder.friendsDrinksInvitationEventSerde())
                        );

        return friendsDrinksInvitationReplyResponses.selectKey((k, v) -> v.getMembershipId());
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-invitation-request.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        if (envProps.getProperty("streams.dir") != null) {
            streamProps.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("streams.dir"));
        }
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        String registryUrl = envProps.getProperty("schema.registry.url");
        RequestService service = new RequestService(
                envProps,
                new andrewgrant.friendsdrinks.AvroBuilder(registryUrl),
                new andrewgrant.friendsdrinks.user.AvroBuilder(registryUrl),
                new andrewgrant.friendsdrinks.frontend.AvroBuilder(registryUrl),
                new andrewgrant.friendsdrinks.membership.AvroBuilder(registryUrl));
        Topology topology = service.buildTopology();
        Properties streamProps = service.buildStreamProperties(envProps);
        KafkaStreams streams = new KafkaStreams(topology, streamProps);
        TopologyDescription description = topology.describe();
        log.info("Topology description: {}", description.toString());

        HttpServer healthCheckServer = andrewgrant.friendsdrinks.health.Server.buildServer(8080, streams);

        log.info("Started streams and the health check server");

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                log.info("Running shutdown hook...");
                andrewgrant.friendsdrinks.health.Server.stop(healthCheckServer);
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            andrewgrant.friendsdrinks.health.Server.start(healthCheckServer);
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
    }

}

class FriendsDrinksMembershipEventConcurrencyCheck {
    FriendsDrinksMembershipApiEvent friendsDrinksMembershipEvent;
    boolean isConcurrentRequest;
}
