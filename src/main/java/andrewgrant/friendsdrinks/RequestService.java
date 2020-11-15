package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;

/**
 * Handles API requests.
 */
public class RequestService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);
    private static final String PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE = "pending-friendsdrinks-requests-store";

    private Properties envProps;
    private AvroBuilder avroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder;

    public RequestService(Properties envProps, AvroBuilder avroBuilder,
                          andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
        this.frontendAvroBuilder = frontendAvroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        final String apiTopicName = envProps.getProperty(FRIENDSDRINKS_API);
        KStream<String, ApiEvent> apiEvents = builder.stream(apiTopicName,
                Consumed.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE),
                Serdes.String(),
                Serdes.String());
        builder.addStateStore(storeBuilder);

        KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_STATE),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        friendsDrinksStateKTable.toStream().process(() -> new Processor<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState>() {
            private KeyValueStore<String, String> stateStore;

            @Override
            public void init(ProcessorContext processorContext) {
                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
            }

            @Override
            public void process(andrewgrant.friendsdrinks.avro.FriendsDrinksId friendsDrinksId, FriendsDrinksState friendsDrinksState) {
                stateStore.delete(friendsDrinksId.getUuid());
            }

            @Override
            public void close() { }
        }, PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);

        KStream<FriendsDrinksId, FriendsDrinksEvent> friendsDrinksApiEvents = apiEvents.filter((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(ApiEventType.FRIENDSDRINKS_EVENT))
                .mapValues(x -> x.getFriendsDrinksEvent())
                .filter(((s, friendsDrinksEvent) -> {
                    FriendsDrinksEventType friendsDrinksEventType = friendsDrinksEvent.getEventType();
                    return friendsDrinksEventType.equals(FriendsDrinksEventType.CREATE_FRIENDSDRINKS_REQUEST) ||
                            friendsDrinksEventType.equals(FriendsDrinksEventType.UPDATE_FRIENDSDRINKS_REQUEST) ||
                            friendsDrinksEventType.equals(FriendsDrinksEventType.DELETE_FRIENDSDRINKS_REQUEST);
                }))
                .selectKey((key, value) ->
                        FriendsDrinksId
                                .newBuilder()
                                .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                                .setUuid(value.getFriendsDrinksId().getUuid())
                                .build());
        KStream<FriendsDrinksId, FriendsDrinksEventConcurrencyCheck> friendsDrinksApiEventsWithConcurrencyCheck =
                checkForConcurrentRequest(friendsDrinksApiEvents);

        KStream<FriendsDrinksId, FriendsDrinksEventConcurrencyCheck>[] friendsDrinksApiEventsBranchedOnConcurrencyCheck =
                friendsDrinksApiEventsWithConcurrencyCheck.branch(
                        (key, value) -> value.isConcurrentRequest,
                        (key, value) -> true
                );

        toRejectedApiEvents(friendsDrinksApiEventsBranchedOnConcurrencyCheck[0].mapValues(x -> x.friendsDrinksEvent))
                .to(apiTopicName, Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));
        KStream<FriendsDrinksId, FriendsDrinksEvent> nonConflictingFriendsDrinksApiEvents =
                friendsDrinksApiEventsBranchedOnConcurrencyCheck[1].mapValues(x -> x.friendsDrinksEvent);

        // Creates.
        KStream<FriendsDrinksId, CreateFriendsDrinksRequest> createRequests = nonConflictingFriendsDrinksApiEvents.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(FriendsDrinksEventType.CREATE_FRIENDSDRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksRequest());
        KStream<FriendsDrinksId, FriendsDrinksEvent> createResponses = handleCreateRequests(createRequests, friendsDrinksStateKTable);
        toApiEventResponse(createResponses).to(apiTopicName, Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        // Updates.
        KStream<FriendsDrinksId, UpdateFriendsDrinksRequest> updateRequests = nonConflictingFriendsDrinksApiEvents.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(FriendsDrinksEventType.UPDATE_FRIENDSDRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getUpdateFriendsDrinksRequest());
        KStream<FriendsDrinksId, FriendsDrinksEvent> updateResponses = handleUpdateRequests(updateRequests, friendsDrinksStateKTable);
        toApiEventResponse(updateResponses).to(apiTopicName, Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        // Deletes.
        KStream<FriendsDrinksId, DeleteFriendsDrinksRequest> deleteRequests = nonConflictingFriendsDrinksApiEvents.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType()
                        .equals(FriendsDrinksEventType.DELETE_FRIENDSDRINKS_REQUEST)))
                .mapValues((friendsDrinksEvent) -> friendsDrinksEvent.getDeleteFriendsDrinksRequest());
        KStream<FriendsDrinksId, FriendsDrinksEvent> deleteResponses = handleDeleteRequests(deleteRequests, friendsDrinksStateKTable);
        toApiEventResponse(deleteResponses).to(apiTopicName, Produced.with(Serdes.String(), frontendAvroBuilder.apiSerde()));

        return builder.build();
    }

    private KStream<String, ApiEvent> toRejectedApiEvents(KStream<FriendsDrinksId, FriendsDrinksEvent> friendsDrinksEventKStream) {
       return friendsDrinksEventKStream.map(((friendsDrinksId, friendsDrinksEvent) -> {
           FriendsDrinksEventType friendsDrinksEventType = friendsDrinksEvent.getEventType();
           String requestId = friendsDrinksEvent.getRequestId();
           FriendsDrinksEvent.Builder responseFriendsDrinksEvent = FriendsDrinksEvent
                   .newBuilder()
                   .setFriendsDrinksId(friendsDrinksEvent.getFriendsDrinksId())
                   .setRequestId(requestId);
           switch (friendsDrinksEventType) {
               case CREATE_FRIENDSDRINKS_REQUEST:
                   responseFriendsDrinksEvent.setEventType(FriendsDrinksEventType.CREATE_FRIENDSDRINKS_RESPONSE);
                   responseFriendsDrinksEvent.setCreateFriendsDrinksResponse(
                           CreateFriendsDrinksResponse
                                   .newBuilder()
                                   .setFriendsDrinksId(friendsDrinksId)
                                   .setRequestId(requestId)
                                   .setResult(Result.FAIL)
                                   .build());
                   break;
               case UPDATE_FRIENDSDRINKS_REQUEST:
                   responseFriendsDrinksEvent.setEventType(FriendsDrinksEventType.UPDATE_FRIENDSDRINKS_RESPONSE);
                   responseFriendsDrinksEvent.setUpdateFriendsDrinksResponse(
                           UpdateFriendsDrinksResponse
                                   .newBuilder()
                                   .setFriendsDrinksId(friendsDrinksId)
                                   .setRequestId(requestId)
                                   .setResult(Result.FAIL)
                                   .build());
                   break;
               case DELETE_FRIENDSDRINKS_REQUEST:
                   responseFriendsDrinksEvent.setEventType(FriendsDrinksEventType.DELETE_FRIENDSDRINKS_RESPONSE);
                   responseFriendsDrinksEvent.setDeleteFriendsDrinksResponse(
                           DeleteFriendsDrinksResponse
                                   .newBuilder()
                                   .setFriendsDrinksId(friendsDrinksId)
                                   .setRequestId(requestId)
                                   .setResult(Result.FAIL)
                                   .build());
                   break;
               default:
                   throw new RuntimeException(String.format("Unexpected event type %s", friendsDrinksEventType.name()));
           }

           ApiEvent.Builder apiEventBuilder = ApiEvent
                   .newBuilder()
                   .setRequestId(friendsDrinksEvent.getRequestId())
                   .setEventType(ApiEventType.FRIENDSDRINKS_EVENT)
                   .setFriendsDrinksEvent(responseFriendsDrinksEvent.build());
           return KeyValue.pair(requestId, apiEventBuilder.build());
       }));
    }

    private KStream<FriendsDrinksId, FriendsDrinksEventConcurrencyCheck> checkForConcurrentRequest(
            KStream<FriendsDrinksId, FriendsDrinksEvent> friendsDrinksEventKStream) {
        return friendsDrinksEventKStream.transformValues(() -> new ValueTransformer<FriendsDrinksEvent, FriendsDrinksEventConcurrencyCheck>() {
           private KeyValueStore<String, String> stateStore;

            @Override
            public void init(ProcessorContext processorContext) {
                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
            }

            @Override
            public FriendsDrinksEventConcurrencyCheck transform(FriendsDrinksEvent friendsDrinksEvent) {
                FriendsDrinksEventConcurrencyCheck concurrencyCheck = new FriendsDrinksEventConcurrencyCheck();
                concurrencyCheck.friendsDrinksEvent = friendsDrinksEvent;
                String uuid = friendsDrinksEvent.getFriendsDrinksId().getUuid();
                if (stateStore.get(uuid) != null) {
                    log.info("Rejecting request {} for FriendsDrinks {} because there's a concurrent request",
                            friendsDrinksEvent.getRequestId(), friendsDrinksEvent.getFriendsDrinksId().getUuid());
                    concurrencyCheck.isConcurrentRequest = true;
                } else {
                    log.info("Grabbing \"lock\" for request {} for FriendsDrinks {}",
                            friendsDrinksEvent.getRequestId(), friendsDrinksEvent.getFriendsDrinksId().getUuid());
                    stateStore.put(uuid, friendsDrinksEvent.getRequestId());
                    concurrencyCheck.isConcurrentRequest = false;
                }
                return concurrencyCheck;
            }

            @Override
            public void close() { }
        }, PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
    }

    private KStream<String, ApiEvent> toApiEventResponse(KStream<FriendsDrinksId, FriendsDrinksEvent> friendsDrinksEventKStream) {
        return friendsDrinksEventKStream.transform(() -> new Transformer<FriendsDrinksId, FriendsDrinksEvent, KeyValue<String, ApiEvent>>() {

            private KeyValueStore<String, String> stateStore;

            @Override
            public void init(ProcessorContext processorContext) {
                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
            }

            @Override
            public KeyValue<String, ApiEvent> transform(FriendsDrinksId friendsDrinksId, FriendsDrinksEvent friendsDrinksEvent) {
                Result result;
                FriendsDrinksEventType friendsDrinksEventType = friendsDrinksEvent.getEventType();
                switch (friendsDrinksEventType) {
                    case CREATE_FRIENDSDRINKS_RESPONSE:
                        result = friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult();
                        break;
                    case UPDATE_FRIENDSDRINKS_RESPONSE:
                        result = friendsDrinksEvent.getUpdateFriendsDrinksResponse().getResult();
                        break;
                    case DELETE_FRIENDSDRINKS_RESPONSE:
                        result = friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult();
                        break;
                    default:
                        throw new RuntimeException(String.format("Unexpected event type %s", friendsDrinksEventType.name()));
                }
                if (result.equals(Result.FAIL)) {
                    log.info("Releasing \"lock\" for FriendsDrinks {}", friendsDrinksEvent.getFriendsDrinksId().getUuid());
                    stateStore.delete(friendsDrinksEvent.getFriendsDrinksId().getUuid());
                }
                ApiEvent apiEvent = ApiEvent
                        .newBuilder()
                        .setEventType(ApiEventType.FRIENDSDRINKS_EVENT)
                        .setFriendsDrinksEvent(friendsDrinksEvent)
                        .setRequestId(friendsDrinksEvent.getRequestId())
                        .build();
                return KeyValue.pair(apiEvent.getRequestId(), apiEvent);
            }

            @Override
            public void close() { }
        }, PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
    }

    private KStream<FriendsDrinksId, FriendsDrinksEvent> handleCreateRequests(
            KStream<FriendsDrinksId, CreateFriendsDrinksRequest> createRequests,
            KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        KTable<String, Long> friendsDrinksCount = friendsDrinksStateKTable.groupBy((key, value) ->
                        KeyValue.pair(value.getFriendsDrinksId().getAdminUserId(), value),
                Grouped.with(Serdes.String(), avroBuilder.friendsDrinksStateSerde()))
                .aggregate(
                        () -> 0L,
                        (aggKey, newValue, aggValue) -> aggValue + 1,
                        (aggKey, oldValue, aggValue) -> aggValue - 1,
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("friendsdrinks-count-state-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );

        return createRequests.selectKey((key, value) -> value.getFriendsDrinksId().getAdminUserId())
                .leftJoin(friendsDrinksCount,
                        (request, count) -> {
                            CreateFriendsDrinksResponse.Builder response = CreateFriendsDrinksResponse.newBuilder();
                            response.setFriendsDrinksId(request.getFriendsDrinksId());
                            response.setRequestId(request.getRequestId());
                            if (count == null || count < 5) {
                                response.setResult(Result.SUCCESS);
                            } else {
                                response.setResult(Result.FAIL);
                            }
                            return FriendsDrinksEvent.newBuilder()
                                    .setEventType(FriendsDrinksEventType.CREATE_FRIENDSDRINKS_RESPONSE)
                                    .setRequestId(response.getRequestId())
                                    .setFriendsDrinksId(request.getFriendsDrinksId())
                                    .setCreateFriendsDrinksResponse(response.build())
                                    .build();
                        },
                        Joined.with(Serdes.String(), frontendAvroBuilder.createFriendsDrinksRequestSerde(), Serdes.Long()))
                .selectKey(((key, value) -> value.getFriendsDrinksId()));
    }

    private KStream<FriendsDrinksId, FriendsDrinksEvent> handleDeleteRequests(
            KStream<FriendsDrinksId, DeleteFriendsDrinksRequest> deleteRequests,
            KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, DeleteFriendsDrinksRequest> deleteRequestsKeyed =
                deleteRequests.selectKey(((key, value) -> andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setUuid(value.getFriendsDrinksId().getUuid()).build()));

        return deleteRequestsKeyed.leftJoin(friendsDrinksStateKTable,
                (request, state) -> {
                    FriendsDrinksEvent friendsDrinksEvent =
                            FriendsDrinksEvent
                                    .newBuilder()
                                    .setEventType(FriendsDrinksEventType.DELETE_FRIENDSDRINKS_RESPONSE)
                                    .setFriendsDrinksId(request.getFriendsDrinksId())
                                    .setRequestId(request.getRequestId())
                                    .setDeleteFriendsDrinksResponse(DeleteFriendsDrinksResponse
                                            .newBuilder()
                                            .setResult(Result.SUCCESS)
                                            .setFriendsDrinksId(request.getFriendsDrinksId())
                                            .setRequestId(request.getRequestId())
                                            .build())
                                    .build();
                    if (state == null) {
                        log.warn(String.format("Failed to find FriendsDrinks state for requestId %s", request.getRequestId()));
                    }
                    return friendsDrinksEvent;
                },
                Joined.with(
                        avroBuilder.friendsDrinksIdSerde(),
                        frontendAvroBuilder.deleteFriendsDrinksRequestSerde(),
                        avroBuilder.friendsDrinksStateSerde()))
                .selectKey((key, value) -> value.getFriendsDrinksId());
    }

    private KStream<FriendsDrinksId, FriendsDrinksEvent> handleUpdateRequests(
            KStream<FriendsDrinksId, UpdateFriendsDrinksRequest> updateRequests,
            KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, UpdateFriendsDrinksRequest> updateRequestsKeyed =
                updateRequests.selectKey(((key, value) -> andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setUuid(value.getFriendsDrinksId().getUuid()).build()));
        return updateRequestsKeyed.leftJoin(friendsDrinksStateKTable,
                (updateRequest, state) -> {
                    if (state != null) {
                        return FriendsDrinksEvent.newBuilder()
                                .setRequestId(updateRequest.getRequestId())
                                .setEventType(FriendsDrinksEventType.UPDATE_FRIENDSDRINKS_RESPONSE)
                                .setFriendsDrinksId(updateRequest.getFriendsDrinksId())
                                .setUpdateFriendsDrinksResponse(
                                        UpdateFriendsDrinksResponse
                                                .newBuilder()
                                                .setRequestId(updateRequest.getRequestId())
                                                .setFriendsDrinksId(updateRequest.getFriendsDrinksId())
                                                .setResult(Result.SUCCESS).build())
                                .build();
                    } else {
                        return FriendsDrinksEvent.newBuilder()
                                .setEventType(FriendsDrinksEventType.UPDATE_FRIENDSDRINKS_RESPONSE)
                                .setUpdateFriendsDrinksResponse(
                                        UpdateFriendsDrinksResponse
                                                .newBuilder()
                                                .setRequestId(updateRequest.getRequestId())
                                                .setFriendsDrinksId(updateRequest.getFriendsDrinksId())
                                                .setResult(Result.FAIL).build())
                                .build();
                    }
                },
                Joined.with(
                        avroBuilder.friendsDrinksIdSerde(),
                        frontendAvroBuilder.updateFriendsDrinksRequestSerde(),
                        avroBuilder.friendsDrinksStateSerde()))
                .selectKey(((key, value) -> value.getFriendsDrinksId()));
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-request.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        String registryUrl = envProps.getProperty("schema.registry.url");
        RequestService service = new RequestService(envProps, new AvroBuilder(registryUrl),
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

class FriendsDrinksEventConcurrencyCheck {
    FriendsDrinksEvent friendsDrinksEvent;
    boolean isConcurrentRequest;
}
