package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
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

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.streamsconfig.Config;

import com.sun.net.httpserver.HttpServer;

/**
 * Handles API requests.
 */
public class RequestService {

    private static final Logger log = LoggerFactory.getLogger(RequestService.class);
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
                Consumed.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE),
                avroBuilder.friendsDrinksIdSerde(),
                Serdes.String());
        builder.addStateStore(storeBuilder);

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_STATE),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        processFriendsDrinksEvents(
                builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_EVENT),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksEventSerde())));


        KStream<FriendsDrinksId, FriendsDrinksApiEvent> friendsDrinksApiRequests =
                apiEvents.filter((s, friendsDrinksEvent) -> friendsDrinksEvent.getEventType().equals(ApiEventType.FRIENDSDRINKS_EVENT))
                        .mapValues(x -> x.getFriendsDrinksEvent())
                        .filter(((s, friendsDrinksEvent) -> {
                            FriendsDrinksApiEventType friendsDrinksEventType = friendsDrinksEvent.getEventType();
                            return friendsDrinksEventType.equals(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_REQUEST) ||
                                    friendsDrinksEventType.equals(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_REQUEST) ||
                                    friendsDrinksEventType.equals(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_REQUEST);
                        }))
                        .selectKey((key, value) -> value.getFriendsDrinksId());

        KStream<FriendsDrinksId, FriendsDrinksEventConcurrencyCheck> friendsDrinksApiEventsWithConcurrencyCheck =
                checkForConcurrentRequest(friendsDrinksApiRequests);

        KStream<FriendsDrinksId, FriendsDrinksEventConcurrencyCheck>[] friendsDrinksApiEventsBranchedOnConcurrencyCheck =
                friendsDrinksApiEventsWithConcurrencyCheck.branch(
                        (key, value) -> value.isConcurrentRequest,
                        (key, value) -> true
                );

        toRejectedApiEventResponses(friendsDrinksApiEventsBranchedOnConcurrencyCheck[0].mapValues(x -> x.friendsDrinksEvent))
                .to(apiTopicName, Produced.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));
        KStream<FriendsDrinksId, FriendsDrinksApiEvent> nonConflictingFriendsDrinksApiEvents =
                friendsDrinksApiEventsBranchedOnConcurrencyCheck[1].mapValues(x -> x.friendsDrinksEvent);

        // Creates.
        KStream<FriendsDrinksId, CreateFriendsDrinksRequest> createRequests = nonConflictingFriendsDrinksApiEvents.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksRequest());
        KStream<FriendsDrinksId, FriendsDrinksApiEvent> createResponses = toCreateResponses(createRequests, friendsDrinksStateKTable);
        toApiEventResponse(createResponses).to(apiTopicName, Produced.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));

        // Updates.
        KStream<FriendsDrinksId, UpdateFriendsDrinksRequest> updateRequests = nonConflictingFriendsDrinksApiEvents.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getUpdateFriendsDrinksRequest());
        KStream<FriendsDrinksId, FriendsDrinksApiEvent> updateResponses = toUpdateResponses(updateRequests, friendsDrinksStateKTable);
        toApiEventResponse(updateResponses).to(apiTopicName, Produced.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));

        // Deletes.
        KStream<FriendsDrinksId, DeleteFriendsDrinksRequest> deleteRequests = nonConflictingFriendsDrinksApiEvents.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_REQUEST)))
                .mapValues((friendsDrinksEvent) -> friendsDrinksEvent.getDeleteFriendsDrinksRequest());
        KStream<FriendsDrinksId, FriendsDrinksApiEvent> deleteResponses = toDeleteResponses(deleteRequests, friendsDrinksStateKTable);
        toApiEventResponse(deleteResponses).to(apiTopicName, Produced.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));

        return builder.build();
    }

    private void processFriendsDrinksEvents(KStream<FriendsDrinksId, FriendsDrinksEvent> friendsDrinksEventKStream) {
        friendsDrinksEventKStream.process(() ->
                new Processor<FriendsDrinksId, FriendsDrinksEvent>() {

                    private KeyValueStore<FriendsDrinksId, String> stateStore;

                    @Override
                    public void init(ProcessorContext processorContext) {
                        stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
                    }

                    @Override
                    public void process(FriendsDrinksId friendsDrinksId, FriendsDrinksEvent friendsDrinksEvent) {
                        String requestId = stateStore.get(friendsDrinksId);
                        if (requestId != null && requestId.equals(friendsDrinksEvent.getRequestId())) {
                            log.info("Deleting request {} from state store UUID {}", requestId, friendsDrinksId.getUuid());
                            stateStore.delete(friendsDrinksId);
                        } else {
                            log.error("Failed to get request {} for FriendsDrinks UUID {}",
                                    requestId, friendsDrinksId.getUuid());
                        }
                    }

                    @Override
                    public void close() { }
                }, PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
    }

    private KStream<String, ApiEvent> toRejectedApiEventResponses(KStream<FriendsDrinksId, FriendsDrinksApiEvent> friendsDrinksEventKStream) {
        return friendsDrinksEventKStream.map(((friendsDrinksId, friendsDrinksEvent) -> {
            FriendsDrinksApiEventType friendsDrinksEventType = friendsDrinksEvent.getEventType();
            String requestId = friendsDrinksEvent.getRequestId();
            FriendsDrinksApiEvent.Builder responseFriendsDrinksEvent = FriendsDrinksApiEvent
                    .newBuilder()
                    .setFriendsDrinksId(friendsDrinksEvent.getFriendsDrinksId())
                    .setRequestId(requestId);
            switch (friendsDrinksEventType) {
                case CREATE_FRIENDSDRINKS_REQUEST:
                    responseFriendsDrinksEvent.setEventType(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_RESPONSE);
                    responseFriendsDrinksEvent.setCreateFriendsDrinksResponse(
                            CreateFriendsDrinksResponse
                                    .newBuilder()
                                    .setRequestId(requestId)
                                    .setResult(Result.FAIL)
                                    .build());
                    break;
                case UPDATE_FRIENDSDRINKS_REQUEST:
                    responseFriendsDrinksEvent.setEventType(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_RESPONSE);
                    responseFriendsDrinksEvent.setUpdateFriendsDrinksResponse(
                            UpdateFriendsDrinksResponse
                                    .newBuilder()
                                    .setRequestId(requestId)
                                    .setResult(Result.FAIL)
                                    .build());
                    break;
                case DELETE_FRIENDSDRINKS_REQUEST:
                    responseFriendsDrinksEvent.setEventType(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_RESPONSE);
                    responseFriendsDrinksEvent.setDeleteFriendsDrinksResponse(
                            DeleteFriendsDrinksResponse
                                    .newBuilder()
                                    .setRequestId(requestId)
                                    .setResult(Result.FAIL)
                                    .build());
                    break;
                default:
                    throw new RuntimeException(String.format("Unexpected event type %s", friendsDrinksEventType.name()));
            }

            ApiEvent apiEvent = ApiEvent
                    .newBuilder()
                    .setRequestId(friendsDrinksEvent.getRequestId())
                    .setEventType(ApiEventType.FRIENDSDRINKS_EVENT)
                    .setFriendsDrinksEvent(responseFriendsDrinksEvent.build())
                    .build();
            return KeyValue.pair(requestId, apiEvent);
        }));
    }

    private KStream<FriendsDrinksId, FriendsDrinksEventConcurrencyCheck> checkForConcurrentRequest(
            KStream<FriendsDrinksId, FriendsDrinksApiEvent> friendsDrinksEventKStream) {
        return friendsDrinksEventKStream.repartition(
                Repartitioned.with(avroBuilder.friendsDrinksIdSerde(), frontendAvroBuilder.friendsDrinksApiEventSerde()))
                .transformValues(() ->
                        new ValueTransformer<FriendsDrinksApiEvent, FriendsDrinksEventConcurrencyCheck>() {
                            private KeyValueStore<FriendsDrinksId, String> stateStore;

                            @Override
                            public void init(ProcessorContext processorContext) {
                                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
                            }

                            @Override
                            public FriendsDrinksEventConcurrencyCheck transform(FriendsDrinksApiEvent friendsDrinksEvent) {
                                FriendsDrinksEventConcurrencyCheck concurrencyCheck = new FriendsDrinksEventConcurrencyCheck();
                                concurrencyCheck.friendsDrinksEvent = friendsDrinksEvent;
                                FriendsDrinksId friendsDrinksId = friendsDrinksEvent.getFriendsDrinksId();
                                if (stateStore.get(friendsDrinksId) != null) {
                                    log.info("Rejecting request {} for FriendsDrinks {} because there's a concurrent request",
                                            friendsDrinksEvent.getRequestId(), friendsDrinksEvent.getFriendsDrinksId().getUuid());
                                    concurrencyCheck.isConcurrentRequest = true;
                                } else {
                                    log.info("Grabbing \"lock\" for request {} for FriendsDrinks UUID {}",
                                            friendsDrinksEvent.getRequestId(), friendsDrinksEvent.getFriendsDrinksId().getUuid());
                                    stateStore.put(friendsDrinksId, friendsDrinksEvent.getRequestId());
                                    concurrencyCheck.isConcurrentRequest = false;
                                }
                                return concurrencyCheck;
                            }

                            @Override
                            public void close() { }
                        }, PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
    }

    private KStream<String, ApiEvent> toApiEventResponse(KStream<FriendsDrinksId, FriendsDrinksApiEvent> friendsDrinksEventKStream) {
        return friendsDrinksEventKStream
                .repartition(Repartitioned.with(avroBuilder.friendsDrinksIdSerde(), frontendAvroBuilder.friendsDrinksApiEventSerde()))
                .transform(() ->
                        new Transformer<FriendsDrinksId, FriendsDrinksApiEvent, KeyValue<String, ApiEvent>>() {

                            private KeyValueStore<FriendsDrinksId, String> stateStore;

                            @Override
                            public void init(ProcessorContext processorContext) {
                                stateStore = (KeyValueStore) processorContext.getStateStore(PENDING_FRIENDSDRINKS_REQUESTS_STATE_STORE);
                            }

                            @Override
                            public KeyValue<String, ApiEvent> transform(FriendsDrinksId friendsDrinksId, FriendsDrinksApiEvent friendsDrinksEvent) {
                                Result result;
                                FriendsDrinksApiEventType friendsDrinksEventType = friendsDrinksEvent.getEventType();
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
                                    String requestId = stateStore.get(friendsDrinksEvent.getFriendsDrinksId());
                                    if (requestId != null && requestId.equals(friendsDrinksEvent.getRequestId())) {
                                        log.info("Releasing \"lock\" for request ID {} FriendsDrinks UUID {}",
                                                requestId, friendsDrinksEvent.getFriendsDrinksId().getUuid());
                                        stateStore.delete(friendsDrinksEvent.getFriendsDrinksId());
                                    } else {
                                        log.error("Failed to get request ID {} for FriendsDrinks UUID {}",
                                                requestId, friendsDrinksId.getUuid());
                                    }
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

    private KStream<FriendsDrinksId, FriendsDrinksApiEvent> toCreateResponses(
            KStream<FriendsDrinksId, CreateFriendsDrinksRequest> createRequests,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        KTable<String, Long> friendsDrinksCount = friendsDrinksStateKTable
                .groupBy((key, value) -> KeyValue.pair(value.getAdminUserId(), value),
                        Grouped.with(Serdes.String(), avroBuilder.friendsDrinksStateSerde()))
                .aggregate(
                        () -> 0L, /* initializer */
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getStatus().equals(FriendsDrinksStatus.ACTIVE)) {
                                return aggValue + 1;
                            } else if (newValue.getStatus().equals(FriendsDrinksStatus.DELETED)) {
                                return aggValue - 1;
                            } else {
                                throw new RuntimeException(String.format("Unknown status %s", newValue.getStatus().name()));
                            }
                        },
                        (aggKey, oldValue, aggValue) -> aggValue - 1,
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>
                                as("aggregated-friendsdrinks-count-table-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()));

        return createRequests.selectKey((key, value) -> value.getAdminUserId())
                .leftJoin(friendsDrinksCount,
                        (request, count) -> {
                            CreateFriendsDrinksResponse.Builder response = CreateFriendsDrinksResponse.newBuilder();
                            response.setRequestId(request.getRequestId());
                            if (count == null || count < 5) {
                                response.setResult(Result.SUCCESS);
                            } else {
                                response.setResult(Result.FAIL);
                            }
                            return FriendsDrinksApiEvent.newBuilder()
                                    .setEventType(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_RESPONSE)
                                    .setRequestId(response.getRequestId())
                                    .setFriendsDrinksId(request.getFriendsDrinksId())
                                    .setCreateFriendsDrinksResponse(response.build())
                                    .build();
                        },
                        Joined.with(Serdes.String(), frontendAvroBuilder.createFriendsDrinksRequestSerde(), Serdes.Long()))
                .selectKey(((key, value) -> value.getFriendsDrinksId()));
    }

    private KStream<FriendsDrinksId, FriendsDrinksApiEvent> toDeleteResponses(
            KStream<FriendsDrinksId, DeleteFriendsDrinksRequest> deleteRequests,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        return deleteRequests.selectKey((key, value) -> value.getFriendsDrinksId())
                .leftJoin(friendsDrinksStateKTable,
                        (request, state) -> {
                            FriendsDrinksApiEvent friendsDrinksEvent =
                                    FriendsDrinksApiEvent
                                            .newBuilder()
                                            .setEventType(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_RESPONSE)
                                            .setFriendsDrinksId(request.getFriendsDrinksId())
                                            .setRequestId(request.getRequestId())
                                            .setDeleteFriendsDrinksResponse(DeleteFriendsDrinksResponse
                                                    .newBuilder()
                                                    .setResult(Result.SUCCESS)
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

    private KStream<FriendsDrinksId, FriendsDrinksApiEvent> toUpdateResponses(
            KStream<FriendsDrinksId, UpdateFriendsDrinksRequest> updateRequests,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        return updateRequests.selectKey((key, value) -> value.getFriendsDrinksId())
                .leftJoin(friendsDrinksStateKTable,
                        (updateRequest, state) -> {
                            if (state != null) {
                                return FriendsDrinksApiEvent.newBuilder()
                                        .setRequestId(updateRequest.getRequestId())
                                        .setEventType(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_RESPONSE)
                                        .setFriendsDrinksId(updateRequest.getFriendsDrinksId())
                                        .setUpdateFriendsDrinksResponse(
                                                UpdateFriendsDrinksResponse
                                                        .newBuilder()
                                                        .setRequestId(updateRequest.getRequestId())
                                                        .setResult(Result.SUCCESS).build())
                                        .build();
                            } else {
                                return FriendsDrinksApiEvent.newBuilder()
                                        .setEventType(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_RESPONSE)
                                        .setFriendsDrinksId(updateRequest.getFriendsDrinksId())
                                        .setUpdateFriendsDrinksResponse(
                                                UpdateFriendsDrinksResponse
                                                        .newBuilder()
                                                        .setRequestId(updateRequest.getRequestId())
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
        String appId = envProps.getProperty("friendsdrinks-request.application.id");
        log.info("App ID is {}", appId);
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        if (envProps.getProperty("streams.dir") != null) {
            streamProps.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("streams.dir"));
        }
        streamProps = Config.addSharedConfig(streamProps);
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
        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception {}", exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
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

        streams.start();
        andrewgrant.friendsdrinks.health.Server.start(healthCheckServer);
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

class FriendsDrinksEventConcurrencyCheck {
    FriendsDrinksApiEvent friendsDrinksEvent;
    boolean isConcurrentRequest;
}
