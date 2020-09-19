package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksCreated;

/**
 * Main FriendsDrinks service.
 */
public class RequestService {

    public Topology buildTopology(Properties envProps, FriendsDrinksAvro avro) {
        StreamsBuilder builder = new StreamsBuilder();
        final String friendsDrinksApiTopicName = envProps.getProperty("friendsdrinks_api.topic.name");

        KStream<FriendsDrinksId, FriendsDrinksEvent> apiEvents = builder.stream(friendsDrinksApiTopicName,
                Consumed.with(avro.apiFriendsDrinksIdSerde(), avro.apiFriendsDrinksSerde()));

        final String friendsDrinksTopicName = envProps.getProperty("friendsdrinks.topic.name");
        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksCreated> currentFriendsDrinks = builder.stream(
                friendsDrinksTopicName,
                Consumed.with(avro.friendsDrinksIdSerde(), avro.friendsDrinksEventSerde()))
                .mapValues((value -> {
                    if (value.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.CREATED)) {
                        return value.getFriendsDrinksCreated();
                    } else if (value.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.DELETED)) {
                        return null;
                    } else {
                        throw new RuntimeException(String.format("Unknown event type %s", value.getEventType().toString()));
                    }
                }));

        KTable<String, Long> friendsDrinksCount = currentFriendsDrinks
                .selectKey((key, value) -> value.getAdminUserId())
                .groupByKey(Grouped.with(Serdes.String(), avro.friendsDrinksCreatedSerde()))
                .aggregate(
                        () -> 0L,
                        (aggKey, newValue, aggValue) -> {
                            if (newValue == null) {
                                return aggValue - 1;
                            } else {
                                return aggValue + 1;
                            }
                        },
                        Materialized.with(Serdes.String(), Serdes.Long())
                );

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
