package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;

/**
 * Main FriendsDrinks service.
 */
public class RequestService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);

    public Topology buildTopology(Properties envProps, FriendsDrinksAvro avro) {
        StreamsBuilder builder = new StreamsBuilder();

        final String apiTopicName = envProps.getProperty("friendsdrinks-api.topic.name");
        KStream<String, FriendsDrinksEvent> apiEvents = builder.stream(apiTopicName,
                Consumed.with(Serdes.String(), avro.apiFriendsDrinksSerde()));

        KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty("friendsdrinks-state.topic.name"),
                        Consumed.with(avro.friendsDrinksIdSerde(), avro.friendsDrinksStateSerde()));

        handleCreateRequests(apiEvents, friendsDrinksStateKTable, avro, apiTopicName);
        handleDeleteRequests(apiEvents, avro, apiTopicName);
        handleUpdateRequests(apiEvents, friendsDrinksStateKTable, avro, apiTopicName);

        String pendingInvitationsTopicName = envProps.getProperty("friendsdrinks-pending-invitation.topic.name");
        KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations = builder.table(
                pendingInvitationsTopicName,
                Consumed.with(avro.friendsDrinksPendingInvitationIdSerde(), avro.friendsDrinksPendingInvitationSerde()));
        handleInvitations(
                apiEvents,
                friendsDrinksStateKTable,
                pendingFriendsDrinksInvitations,
                avro,
                apiTopicName,
                pendingInvitationsTopicName);


        return builder.build();
    }

    private KTable<String, Long> tableOfFriendsDrinksCount(
            KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> kTable,
            FriendsDrinksAvro avro) {

        return kTable.groupBy((key, value) -> KeyValue.pair(value.getFriendsDrinksId().getAdminUserId(), value),
                Grouped.with(Serdes.String(), avro.friendsDrinksStateSerde()))
                .aggregate(
                        () -> 0L,
                        (aggKey, newValue, aggValue) -> {
                            Long newAggValue = aggValue + 1;
                            log.info("new value {}. New aggValue {}", newValue, newAggValue);
                            return newAggValue;
                        },
                        (aggKey, oldValue, aggValue) -> {
                            Long newAggValue = aggValue - 1;
                            log.info("old value {}. New aggValue {}", oldValue, newAggValue);
                            return newAggValue;
                        },
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("internal_request_service_friendsdrinks-count_tracker")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );
    }

    private void handleCreateRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                      KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                      FriendsDrinksAvro avro,
                                      String apiTopicName) {

        KStream<String, CreateFriendsDrinksRequest> createRequests = apiEvents
                .filter(((s, friendsDrinksEvent) -> friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_REQUEST)))
                .selectKey((key, value) -> value.getCreateFriendsDrinksRequest().getFriendsDrinksId().getAdminUserId())
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksRequest());

        KTable<String, Long> friendsDrinksCount = tableOfFriendsDrinksCount(friendsDrinksStateKTable, avro);

        KStream<String, FriendsDrinksEvent> createResponses = createRequests.leftJoin(friendsDrinksCount,
                (request, count) -> {
                    CreateFriendsDrinksResponse.Builder response = CreateFriendsDrinksResponse.newBuilder();
                    response.setRequestId(request.getRequestId());
                    if (count == null || count < 5) {
                        response.setResult(Result.SUCCESS);
                    } else {
                        response.setResult(Result.FAIL);
                    }
                    FriendsDrinksEvent event = FriendsDrinksEvent.newBuilder()
                            .setEventType(EventType.CREATE_FRIENDSDRINKS_RESPONSE)
                            .setRequestId(response.getRequestId())
                            .setCreateFriendsDrinksResponse(response.build())
                            .build();
                    return event;
                },
                Joined.with(Serdes.String(), avro.createFriendsDrinksRequestSerde(), Serdes.Long()))
                .selectKey(((key, value) -> value.getCreateFriendsDrinksResponse().getRequestId()));

        createResponses.to(apiTopicName,
                Produced.with(Serdes.String(), avro.apiFriendsDrinksSerde()));
    }

    private void handleDeleteRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                      FriendsDrinksAvro avro,
                                      String apiTopicName) {

        // Deletes
        apiEvents.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_REQUEST)))
                .mapValues((friendsDrinksEvent) -> friendsDrinksEvent.getDeleteFriendsDrinksRequest())
                .mapValues((request) -> FriendsDrinksEvent.newBuilder()
                        .setEventType(EventType.DELETE_FRIENDSDRINKS_RESPONSE)
                        .setRequestId(request.getRequestId())
                        .setDeleteFriendsDrinksResponse(DeleteFriendsDrinksResponse
                                .newBuilder()
                                .setResult(Result.SUCCESS)
                                .setRequestId(request.getRequestId())
                                .build())
                        .build())
                .to(apiTopicName,
                        Produced.with(Serdes.String(), avro.apiFriendsDrinksSerde()));
    }

    private void handleUpdateRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                      KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                      FriendsDrinksAvro avro,
                                      String apiTopicName) {

        // Updates
        KStream<String, UpdateFriendsDrinksRequest> updateRequests = apiEvents
                .filter(((s, friendsDrinksEvent) -> friendsDrinksEvent.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getUpdateFriendsDrinksRequest());
        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, UpdateFriendsDrinksRequest> updateRequestsKeyed =
                updateRequests.selectKey(((key, value) -> andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setFriendsDrinksId(value.getFriendsDrinksId().getFriendsDrinksId()).build()));
        KStream<String, FriendsDrinksEvent> updateResponses = updateRequestsKeyed.leftJoin(friendsDrinksStateKTable,
                (updateRequest, state) -> {
                    if (state != null) {
                        return FriendsDrinksEvent.newBuilder()
                                .setEventType(EventType.UPDATE_FRIENDSDRINKS_RESPONSE)
                                .setRequestId(updateRequest.getRequestId())
                                .setUpdateFriendsDrinksResponse(
                                        UpdateFriendsDrinksResponse
                                                .newBuilder()
                                                .setRequestId(updateRequest.getRequestId())
                                                .setResult(Result.SUCCESS).build())
                                .build();
                    } else {
                        return FriendsDrinksEvent.newBuilder()
                                .setRequestId(updateRequest.getRequestId())
                                .setEventType(EventType.UPDATE_FRIENDSDRINKS_RESPONSE)
                                .setUpdateFriendsDrinksResponse(
                                        UpdateFriendsDrinksResponse
                                                .newBuilder()
                                                .setRequestId(updateRequest.getRequestId())
                                                .setResult(Result.FAIL).build())
                                .build();
                    }
                },
                Joined.with(avro.friendsDrinksIdSerde(), avro.updateFriendsDrinksRequestSerde(), avro.friendsDrinksStateSerde()))
                .selectKey(((key, value) -> value.getUpdateFriendsDrinksResponse().getRequestId()));
        updateResponses.to(apiTopicName, Produced.with(Serdes.String(), avro.apiFriendsDrinksSerde()));

    }

    private void handleInvitations(
            KStream<String, FriendsDrinksEvent> apiEvents,
            KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
            KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations,
            FriendsDrinksAvro avro,
            String apiTopicName,
            String pendingInvitationsTopicName) {

        handleInvitationRequests(apiEvents, friendsDrinksStateKTable, avro, apiTopicName, pendingInvitationsTopicName);
        handleInvitationReplies(apiEvents, friendsDrinksStateKTable,
                pendingFriendsDrinksInvitations, avro, apiTopicName, pendingInvitationsTopicName);

    }

    private void handleInvitationRequests(
            KStream<String, FriendsDrinksEvent> apiEvents,
            KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
            FriendsDrinksAvro avro,
            String apiTopicName,
            String pendingInvitationsTopicName) {
        // FriendsDrinks invitation requests
        KStream<String, CreateFriendsDrinksInvitationRequest> friendsDrinksInvitations = apiEvents
                .filter((key, value) -> value.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_INVITATION_REQUEST))
                .mapValues(value -> value.getCreateFriendsDrinksInvitationRequest());
        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, CreateFriendsDrinksInvitationAggregateResult>
                createFriendsDrinksInvitationAggregateResult = friendsDrinksInvitations.selectKey((key, value) ->
                andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setFriendsDrinksId(value.getFriendsDrinksId().getFriendsDrinksId())
                        .build())
                .leftJoin(friendsDrinksStateKTable,
                        (request, state) -> {
                            CreateFriendsDrinksInvitationAggregateResult aggregateResult =
                                    new CreateFriendsDrinksInvitationAggregateResult();
                            if (state != null) {
                                if (state.getUserIds().contains(request.getUserId()) ||
                                        state.getFriendsDrinksId().getAdminUserId().equals(request.getUserId().getUserId())) {
                                    aggregateResult.failed = true;
                                } else {
                                    // Confirms the FriendsDrinks exists.
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
                                    aggregateResult.createFriendsDrinksInvitationRequest = request;
                                    aggregateResult.friendsDrinksPendingInvitation = pendingInvitation;
                                    aggregateResult.failed = false;
                                }
                            } else {
                                aggregateResult.failed = true;
                            }
                            return aggregateResult;
                        },
                        Joined.with(avro.friendsDrinksIdSerde(), avro.createFriendsDrinksInvitationRequestSerde(), avro.friendsDrinksStateSerde())
                );

        createFriendsDrinksInvitationAggregateResult.filter((key, value) -> value.failed == false).map((key, value) ->
                new KeyValue<>(value.friendsDrinksPendingInvitation.getInvitationId(), value.friendsDrinksPendingInvitation))
                .to(pendingInvitationsTopicName,
                        Produced.with(
                                avro.friendsDrinksPendingInvitationIdSerde(),
                                avro.friendsDrinksPendingInvitationSerde()));

        createFriendsDrinksInvitationAggregateResult.map((key, value) -> {
            CreateFriendsDrinksInvitationResponse response;
            if (value.failed) {
                response = CreateFriendsDrinksInvitationResponse
                        .newBuilder()
                        .setRequestId(value.createFriendsDrinksInvitationRequest.getRequestId())
                        .setResult(Result.FAIL)
                        .build();
            } else {
                response = CreateFriendsDrinksInvitationResponse
                        .newBuilder()
                        .setRequestId(value.createFriendsDrinksInvitationRequest.getRequestId())
                        .setResult(Result.SUCCESS)
                        .build();
            }
            FriendsDrinksEvent friendsDrinksEvent = FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(EventType.CREATE_FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setRequestId(value.createFriendsDrinksInvitationRequest.getRequestId())
                    .setCreateFriendsDrinksInvitationResponse(response)
                    .build();
            return new KeyValue<>(
                    friendsDrinksEvent.getCreateFriendsDrinksInvitationResponse().getRequestId(),
                    friendsDrinksEvent);
        }).to(apiTopicName, Produced.with(Serdes.String(), avro.apiFriendsDrinksSerde()));
    }

    private void handleInvitationReplies(
            KStream<String, FriendsDrinksEvent> apiEvents,
            KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
            KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations,
            FriendsDrinksAvro avro,
            String apiTopicName,
            String pendingInvitationsTopicName) {
        // FriendsDrinks replies
        KStream<String, CreateFriendsDrinksInvitationReplyRequest> createFriendsDrinksInvitationReplyRequests = apiEvents
                .filter((key, value) -> value.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                .mapValues(value -> value.getCreateFriendsDrinksInvitationReplyRequest());
        KStream<FriendsDrinksPendingInvitationId, CreateFriendsDrinksInvitationReplyResponse> createFriendsDrinksInvitationReplyResponses =
                createFriendsDrinksInvitationReplyRequests.selectKey((key, value) -> FriendsDrinksPendingInvitationId
                        .newBuilder()
                        .setFriendsDrinksId(value.getFriendsDrinksId())
                        .setUserId(value.getUserId())
                        .build())
                        .leftJoin(pendingFriendsDrinksInvitations,
                                (request, state) -> {
                                    if (state != null) {
                                        return CreateFriendsDrinksInvitationReplyResponse
                                                .newBuilder()
                                                .setRequestId(request.getRequestId())
                                                .setResult(Result.SUCCESS)
                                                .build();
                                    } else {
                                        return CreateFriendsDrinksInvitationReplyResponse
                                                .newBuilder()
                                                .setRequestId(request.getRequestId())
                                                .setResult(Result.FAIL)
                                                .build();
                                    }
                                },
                                Joined.with(
                                        avro.friendsDrinksPendingInvitationIdSerde(),
                                        avro.createFriendsDrinksInvitationReplyRequestSerde(), avro.friendsDrinksPendingInvitationSerde())
                        );

        createFriendsDrinksInvitationReplyResponses.selectKey(((key, value) -> value.getRequestId()))
                .mapValues(value -> FriendsDrinksEvent
                        .newBuilder()
                        .setEventType(EventType.CREATE_FRIENDSDRINKS_INVITATION_REPLY_RESPONSE)
                        .setRequestId(value.getRequestId())
                        .setCreateFriendsDrinksInvitationReplyResponse(value)
                        .build())
                .to(apiTopicName, Produced.with(Serdes.String(), avro.apiFriendsDrinksSerde()));

        createFriendsDrinksInvitationReplyResponses.filter((key, value) -> value.getResult().equals(Result.SUCCESS))
                .mapValues(value -> (FriendsDrinksPendingInvitation) null)
                .to(pendingInvitationsTopicName,
                        Produced.with(avro.friendsDrinksPendingInvitationIdSerde(), avro.friendsDrinksPendingInvitationSerde()));
    }


    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-request.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        RequestService service = new RequestService();
        Topology topology = service.buildTopology(envProps,
                new FriendsDrinksAvro(envProps.getProperty("schema.registry.url")));
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
