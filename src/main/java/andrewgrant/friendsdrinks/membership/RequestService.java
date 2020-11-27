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

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.avro.Status;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksInvitationEvent;
import andrewgrant.friendsdrinks.user.AvroBuilder;
import andrewgrant.friendsdrinks.user.avro.UserId;
import andrewgrant.friendsdrinks.user.avro.UserState;


/**
 * Processes invitation requests.
 */
public class RequestService {

    private static final Logger log = LoggerFactory.getLogger(RequestService.class);
    private static final String PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE =
            "pending-friendsdrinks-membership-requests-store";

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

        KTable<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> friendsDrinksInvitations =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION_EVENT),
                        Consumed.with(membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                                membershipAvroBuilder.friendsDrinksInvitationEventSerde()));

        KStream<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId,
                andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipEvent> membershipEventKStream =
                builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                        Consumed.with(
                                membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                                membershipAvroBuilder.friendsDrinksMembershipEventSerdes()));

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE),
                membershipAvroBuilder.friendsDrinksMembershipIdSerdes(),
                Serdes.String());
        builder.addStateStore(storeBuilder);

        purgePendingRequestsStore(membershipEventKStream, friendsDrinksInvitations.toStream());

        KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> friendsDrinksMembershipEventKStream =
                apiEvents.filter((key, value) -> value.getEventType().equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT))
                        .mapValues(x -> x.getFriendsDrinksMembershipEvent())
                        .filter((key, value) -> {
                            FriendsDrinksMembershipEventType eventType = value.getEventType();
                            return eventType.equals(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST) ||
                                    eventType.equals(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REQUEST);
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
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        InvitationRequestResult invitationRequestResult =
                handleInvitationRequests(branchedConcurrencyCheck[1]
                        .mapValues(v -> v.friendsDrinksMembershipEvent)
                        .filter((key, value) ->
                                value.getEventType().equals(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REQUEST))
                        .mapValues(v -> v.getFriendsDrinksInvitationRequest()), friendsDrinksStateKTable, userState);

        toApiResponse(invitationRequestResult.getSuccessfulResponseKStream())
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        for (KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> friendsDrinksEventKStream :
                invitationRequestResult.getFailedResponseKStreams()) {
            toApiResponse(friendsDrinksEventKStream)
                    .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));
        }

        toApiResponse(handleInvitationReplies(branchedConcurrencyCheck[1]
                .mapValues(v -> v.friendsDrinksMembershipEvent)
                .filter((key, value) ->
                        value.getEventType().equals(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                .mapValues(v -> v.getFriendsDrinksInvitationReplyRequest()),  friendsDrinksInvitations))
                .to(envProps.getProperty(FRIENDSDRINKS_API), Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        return builder.build();
    }

    private void purgePendingRequestsStore(
            KStream<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId,
                    andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipEvent> membershipEventKStream,
            KStream<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksInvitationEvent>
                    friendsDrinksInvitationKStream) {
        membershipEventKStream.selectKey((key, value) -> toApi(key))
                .process(() ->
                        new Processor<FriendsDrinksMembershipId,
                                andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipEvent>() {

                            private KeyValueStore<FriendsDrinksMembershipId, String> stateStore;

                            @Override
                            public void init(ProcessorContext processorContext) {
                                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
                            }

                            @Override
                            public void process(
                                    FriendsDrinksMembershipId friendsDrinksMembershipId,
                                    andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipEvent friendsDrinksMembershipEvent) {
                                String requestId = stateStore.get(friendsDrinksMembershipId);
                                if (requestId != null && requestId.equals(friendsDrinksMembershipEvent.getRequestId())) {
                                    stateStore.delete(friendsDrinksMembershipId);
                                } else {
                                    log.error("Failed to get request for FriendsDrinks UUID {} Admin ID {}",
                                            friendsDrinksMembershipId.getFriendsDrinksId().getUuid(),
                                            friendsDrinksMembershipId.getFriendsDrinksId().getAdminUserId());
                                }
                            }

                            @Override
                            public void close() { }
                        }, PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);

        friendsDrinksInvitationKStream.selectKey((key, value) -> toApi(key)).process(() ->
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
                            log.error("Failed to get request {} for FriendsDrinks UUID {} Admin ID {}",
                                    friendsDrinksInvitation.getRequestId(),
                                    friendsDrinksMembershipId.getFriendsDrinksId().getUuid(),
                                    friendsDrinksMembershipId.getFriendsDrinksId().getAdminUserId());
                        }
                    }

                    @Override
                    public void close() { }
                }, PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
    }

    private KStream<String, ApiEvent> toRejectedResponse(
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> friendsDrinksMembershipEventKStream) {
        return friendsDrinksMembershipEventKStream.map((key, value) -> {
            FriendsDrinksMembershipEventType eventType = value.getEventType();
            ApiEvent.Builder apiEventBuilder = ApiEvent
                    .newBuilder()
                    .setRequestId(value.getRequestId())
                    .setEventType(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT);
            String requestId = value.getRequestId();
            switch (eventType) {
                case FRIENDSDRINKS_INVITATION_REQUEST:
                    apiEventBuilder.setFriendsDrinksMembershipEvent(
                            FriendsDrinksMembershipEvent
                                    .newBuilder()
                                    .setRequestId(requestId)
                                    .setEventType(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_RESPONSE)
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
                            FriendsDrinksMembershipEvent
                                    .newBuilder()
                                    .setEventType(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE)
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
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> friendsDrinksMembershipEventKStream
    ) {
        return friendsDrinksMembershipEventKStream.transformValues(() ->
                new ValueTransformer<FriendsDrinksMembershipEvent, FriendsDrinksMembershipEventConcurrencyCheck>() {

                    private KeyValueStore<FriendsDrinksMembershipId, String> stateStore;

                    @Override
                    public void init(ProcessorContext processorContext) {
                        stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
                    }

                    @Override
                    public FriendsDrinksMembershipEventConcurrencyCheck transform(FriendsDrinksMembershipEvent friendsDrinksMembershipEvent) {
                        FriendsDrinksMembershipEventConcurrencyCheck concurrencyCheck =
                                new FriendsDrinksMembershipEventConcurrencyCheck();
                        concurrencyCheck.friendsDrinksMembershipEvent = friendsDrinksMembershipEvent;
                        FriendsDrinksMembershipId friendsDrinksMembershipId = friendsDrinksMembershipEvent.getMembershipId();
                        if (stateStore.get(friendsDrinksMembershipId) != null) {
                            concurrencyCheck.isConcurrentRequest = true;
                            log.info("Found a concurrent request for FriendsDrinks UUID {} Admin ID {} and User ID {} with request ID {}",
                                    friendsDrinksMembershipId.getFriendsDrinksId().getUuid(),
                                    friendsDrinksMembershipId.getFriendsDrinksId().getAdminUserId(),
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
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> friendsDrinksMembershipEventKStream) {

        return friendsDrinksMembershipEventKStream.transform(() ->
                new Transformer<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent, KeyValue<String, ApiEvent>>() {

                    private KeyValueStore<FriendsDrinksMembershipId, String> stateStore;

                    @Override
                    public void init(ProcessorContext processorContext) {
                        stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_MEMBERSHIP_REQUESTS_STATE_STORE);
                    }

                    @Override
                    public KeyValue<String, ApiEvent> transform(
                            FriendsDrinksMembershipId friendsDrinksMembershipId,
                            FriendsDrinksMembershipEvent friendsDrinksMembershipEvent) {
                        Result result;
                        FriendsDrinksMembershipEventType eventType = friendsDrinksMembershipEvent.getEventType();
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
                                log.error("Failed to get request for FriendsDrinks UUID Admin ID {}",
                                        friendsDrinksMembershipId.getFriendsDrinksId().getUuid(),
                                        friendsDrinksMembershipId.getFriendsDrinksId().getAdminUserId());
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
            KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
            KTable<UserId, UserState> userState) {

        KStream<FriendsDrinksMembershipId, InvitationResult> resultsAfterValidatingFriendsDrinksState =
                friendsDrinksInvitations.selectKey((key, value) ->
                        andrewgrant.friendsdrinks.avro.FriendsDrinksId
                                .newBuilder()
                                .setAdminUserId(value.getMembershipId().getFriendsDrinksId().getAdminUserId())
                                .setUuid(value.getMembershipId().getFriendsDrinksId().getUuid())
                                .build())
                        .leftJoin(friendsDrinksStateKTable,
                                (request, state) -> {
                                    InvitationResult invitationResult = new InvitationResult();
                                    invitationResult.invitationRequest = request;
                                    if (request.getMembershipId().getUserId().getUserId().equals(
                                            request.getMembershipId().getFriendsDrinksId().getAdminUserId())) {
                                        invitationResult.failed = true;
                                        return invitationResult;
                                    }
                                    // Validate request against state of FriendsDrinks
                                    if (state != null && (!state.getStatus().equals(Status.DELETED))) {
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
                        Joined.with(userAvroBuilder.userIdSerde(), frontendAvroBuilder.friendsDrinksInvitationRequestSerde(),
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

            FriendsDrinksMembershipEvent friendsDrinksMembershipEvent = FriendsDrinksMembershipEvent.newBuilder()
                    .setEventType(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_RESPONSE)
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

        private List<KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent>> failedResponseKStreams;
        private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> successfulResponseKStream;

        public InvitationRequestResult() {
            this.failedResponseKStreams = new ArrayList<>();
            this.successfulResponseKStream = null;
        }

        public List<KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent>> getFailedResponseKStreams() {
            return failedResponseKStreams;
        }

        public KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> getSuccessfulResponseKStream() {
            return successfulResponseKStream;
        }

        public void addFailedResponse(KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> failedResponse) {
            failedResponseKStreams.add(failedResponse);
        }

        public void setSuccessfulResponseKStream(KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> successfulResponseKStream) {
            this.successfulResponseKStream = successfulResponseKStream;
        }
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> convertToFailedInvitationResponse(
            KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationRequest> stream) {
        return stream.map((key, value) -> {
            FriendsDrinksInvitationResponse response = FriendsDrinksInvitationResponse
                    .newBuilder()
                    .setResult(Result.FAIL)
                    .setRequestId(value.getRequestId())
                    .build();

            FriendsDrinksMembershipEvent friendsDrinksMembershipEvent = FriendsDrinksMembershipEvent.newBuilder()
                    .setEventType(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setFriendsDrinksInvitationResponse(response)
                    .setRequestId(response.getRequestId())
                    .setMembershipId(value.getMembershipId())
                    .setMembershipId(value.getMembershipId())
                    .build();

            return KeyValue.pair(friendsDrinksMembershipEvent.getMembershipId(), friendsDrinksMembershipEvent);
        });
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> handleInvitationReplies(
            KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationReplyRequest> invitationReplyRequestKStream,
            KTable<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> friendsDrinksInvitations) {
        KStream<andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId, FriendsDrinksMembershipEvent>
                friendsDrinksInvitationReplyResponses =
                invitationReplyRequestKStream.selectKey((key, value) -> andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId
                        .newBuilder()
                        .setFriendsDrinksId(andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId
                                .newBuilder()
                                .setUuid(value.getMembershipId().getFriendsDrinksId().getUuid())
                                .setAdminUserId(value.getMembershipId().getFriendsDrinksId().getAdminUserId())
                                .build())
                        .setUserId(andrewgrant.friendsdrinks.membership.avro.UserId
                                .newBuilder()
                                .setUserId(value.getMembershipId().getUserId().getUserId())
                                .build())
                        .build())
                        .leftJoin(friendsDrinksInvitations,
                                (request, state) -> {
                                    FriendsDrinksMembershipEvent.Builder friendsDrinksMembershipEvent =
                                            FriendsDrinksMembershipEvent.newBuilder()
                                                    .setRequestId(request.getRequestId())
                                                    .setMembershipId(request.getMembershipId())
                                                    .setEventType(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE);
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

    private FriendsDrinksMembershipId toApi(andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId friendsDrinksMembershipId) {
        return FriendsDrinksMembershipId
                .newBuilder()
                .setUserId(
                        andrewgrant.friendsdrinks.api.avro.UserId
                                .newBuilder()
                                .setUserId(friendsDrinksMembershipId.getUserId().getUserId())
                                .build())
                .setFriendsDrinksId(andrewgrant.friendsdrinks.api.avro.FriendsDrinksId
                        .newBuilder()
                        .setUuid(friendsDrinksMembershipId.getFriendsDrinksId().getUuid())
                        .setAdminUserId(friendsDrinksMembershipId.getFriendsDrinksId().getAdminUserId())
                        .build())
                .build();
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-invitation-request.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
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

class FriendsDrinksMembershipEventConcurrencyCheck {
    FriendsDrinksMembershipEvent friendsDrinksMembershipEvent;
    boolean isConcurrentRequest;
}
