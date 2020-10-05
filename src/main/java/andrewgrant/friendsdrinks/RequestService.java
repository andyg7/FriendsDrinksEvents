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
 * Handles API requests.
 */
public class RequestService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);

    public Topology buildTopology(Properties envProps, AvroBuilder avroBuilder,
                                  andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder) {
        StreamsBuilder builder = new StreamsBuilder();

        final String apiTopicName = envProps.getProperty("friendsdrinks-api.topic.name");
        KStream<String, FriendsDrinksEvent> apiEvents = builder.stream(apiTopicName,
                Consumed.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));

        KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty("friendsdrinks-state.topic.name"),
                        Consumed.with(avroBuilder.friendsDrinksIdSerde(), avroBuilder.friendsDrinksStateSerde()));

        handleCreateRequests(apiEvents, friendsDrinksStateKTable, avroBuilder, apiAvroBuilder, apiTopicName);
        handleDeleteRequests(apiEvents, friendsDrinksStateKTable, avroBuilder, apiAvroBuilder, apiTopicName);
        handleUpdateRequests(apiEvents, friendsDrinksStateKTable, avroBuilder, apiAvroBuilder, apiTopicName);

        return builder.build();
    }

    private void handleCreateRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                      KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                      AvroBuilder avro,
                                      andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                      String apiTopicName) {

        KTable<String, Long> friendsDrinksCount = friendsDrinksStateKTable.groupBy((key, value) ->
                        KeyValue.pair(value.getFriendsDrinksId().getAdminUserId(), value),
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

        KStream<String, CreateFriendsDrinksRequest> createRequests = apiEvents
                .filter(((s, friendsDrinksEvent) -> friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_REQUEST)))
                .selectKey((key, value) -> value.getCreateFriendsDrinksRequest().getFriendsDrinksId().getAdminUserId())
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksRequest());

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
                Joined.with(Serdes.String(), apiAvroBuilder.createFriendsDrinksRequestSerde(), Serdes.Long()))
                .selectKey(((key, value) -> value.getCreateFriendsDrinksResponse().getRequestId()));

        createResponses.to(apiTopicName,
                Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));
    }

    private void handleDeleteRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                      KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                      AvroBuilder avro,
                                      andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                      String apiTopicName) {

        KStream<String, DeleteFriendsDrinksRequest> deleteRequests =
                apiEvents.filter(((s, friendsDrinksEvent) ->
                        friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_REQUEST)))
                        .mapValues((friendsDrinksEvent) -> friendsDrinksEvent.getDeleteFriendsDrinksRequest());

        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, DeleteFriendsDrinksRequest> deleteRequestsKeyed =
                deleteRequests.selectKey(((key, value) -> andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setUuid(value.getFriendsDrinksId().getUuid()).build()));

        deleteRequestsKeyed.leftJoin(friendsDrinksStateKTable,
                (request, state) -> {
                    if (state != null) {
                        return FriendsDrinksEvent
                                .newBuilder()
                                .setEventType(EventType.DELETE_FRIENDSDRINKS_RESPONSE)
                                .setRequestId(request.getRequestId())
                                .setDeleteFriendsDrinksResponse(DeleteFriendsDrinksResponse
                                        .newBuilder()
                                        .setResult(Result.SUCCESS)
                                        .setRequestId(request.getRequestId())
                                        .build())
                                .build();

                    } else {
                        return FriendsDrinksEvent
                                .newBuilder()
                                .setEventType(EventType.DELETE_FRIENDSDRINKS_RESPONSE)
                                .setRequestId(request.getRequestId())
                                .setDeleteFriendsDrinksResponse(DeleteFriendsDrinksResponse
                                        .newBuilder()
                                        .setResult(Result.FAIL)
                                        .setRequestId(request.getRequestId())
                                        .build())
                                .build();
                    }
                },
                Joined.with(avro.friendsDrinksIdSerde(), apiAvroBuilder.deleteFriendsDrinksRequestSerde(), avro.friendsDrinksStateSerde()))
                .selectKey((key, value) -> value.getRequestId())
                .to(apiTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));
    }

    private void handleUpdateRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                      KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                      AvroBuilder avro,
                                      andrewgrant.friendsdrinks.frontend.restapi.AvroBuilder apiAvroBuilder,
                                      String apiTopicName) {

        // Updates
        KStream<String, UpdateFriendsDrinksRequest> updateRequests = apiEvents
                .filter(((s, friendsDrinksEvent) -> friendsDrinksEvent.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getUpdateFriendsDrinksRequest());
        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, UpdateFriendsDrinksRequest> updateRequestsKeyed =
                updateRequests.selectKey(((key, value) -> andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setUuid(value.getFriendsDrinksId().getUuid()).build()));
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
                Joined.with(avro.friendsDrinksIdSerde(), apiAvroBuilder.updateFriendsDrinksRequestSerde(), avro.friendsDrinksStateSerde()))
                .selectKey(((key, value) -> value.getUpdateFriendsDrinksResponse().getRequestId()));
        updateResponses.to(apiTopicName, Produced.with(Serdes.String(), apiAvroBuilder.apiFriendsDrinksSerde()));

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
        String registryUrl = envProps.getProperty("schema.registry.url");
        Topology topology = service.buildTopology(envProps, new AvroBuilder(registryUrl),
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
