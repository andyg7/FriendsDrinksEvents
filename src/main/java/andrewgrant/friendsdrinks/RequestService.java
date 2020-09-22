package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;

/**
 * Main FriendsDrinks service.
 */
public class RequestService {

    public Topology buildTopology(Properties envProps, FriendsDrinksAvro avro) {
        StreamsBuilder builder = new StreamsBuilder();

        final String friendsDrinksApiTopicName = envProps.getProperty("friendsdrinks_api.topic.name");
        KStream<FriendsDrinksId, FriendsDrinksEvent> apiEvents = builder.stream(friendsDrinksApiTopicName,
                Consumed.with(avro.apiFriendsDrinksIdSerde(), avro.apiFriendsDrinksSerde()));

        KTable<String, Long> friendsDrinksCount =
                builder.table(envProps.getProperty("friendsdrinks_state.topic.name"),
                        Consumed.with(avro.friendsDrinksIdSerde(), avro.friendsDrinksStateSerde()))
                        .groupBy(
                                (key, value) -> KeyValue.pair(value.getAdminUserId(), value),
                                Grouped.with(Serdes.String(), avro.friendsDrinksStateSerde()))
                        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("internal_request_service_friendsdrinks_count_tracker")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()));

        KStream<String, CreateFriendsDrinksRequest> createRequests = apiEvents
                .filter(((s, friendsDrinksEvent) -> friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_REQUEST)))
                .selectKey((key, value) -> value.getCreateFriendsDrinksRequest().getAdminUserId())
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksRequest());

        KStream<FriendsDrinksId, FriendsDrinksEvent> createResponses = createRequests.leftJoin(friendsDrinksCount,
                (request, count) -> {
                    CreateFriendsDrinksResponse.Builder response = CreateFriendsDrinksResponse.newBuilder();
                    response.setRequestId(request.getRequestId());
                    response.setFriendsDrinksId(request.getFriendsDrinksId());
                    if (count == null || count < 5) {
                        response.setResult(Result.SUCCESS);
                    } else {
                        response.setResult(Result.FAIL);
                    }
                    FriendsDrinksEvent event = FriendsDrinksEvent.newBuilder()
                            .setEventType(EventType.CREATE_FRIENDS_DRINKS_RESPONSE)
                            .setCreateFriendsDrinksResponse(response.build())
                            .build();
                    return event;
                },
                Joined.with(Serdes.String(), avro.createFriendsDrinksRequestSerde(), Serdes.Long()))
                .selectKey(((key, value) -> value.getCreateFriendsDrinksResponse().getFriendsDrinksId()));

        createResponses.to(friendsDrinksApiTopicName,
                Produced.with(avro.apiFriendsDrinksIdSerde(), avro.apiFriendsDrinksSerde()));

        apiEvents.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_REQUEST)))
                .mapValues((friendsDrinksEvent) -> friendsDrinksEvent.getDeleteFriendsDrinksRequest())
                .mapValues((request) -> FriendsDrinksEvent.newBuilder()
                        .setEventType(EventType.DELETE_FRIENDS_DRINKS_RESPONSE)
                        .setDeleteFriendsDrinksResponse(DeleteFriendsDrinksResponse
                                .newBuilder()
                                .setResult(Result.SUCCESS)
                                .setRequestId(request.getRequestId())
                                .build())
                        .build())
                .to(friendsDrinksApiTopicName,
                        Produced.with(avro.apiFriendsDrinksIdSerde(), avro.apiFriendsDrinksSerde()));

        return builder.build();
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks_request.application.id"));
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
