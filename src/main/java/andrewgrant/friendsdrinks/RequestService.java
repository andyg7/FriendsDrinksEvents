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

import andrewgrant.friendsdrinks.avro.*;

/**
 * Main FriendsDrinks service.
 */
public class RequestService {

    public Topology buildTopology(Properties envProps, FriendsDrinksAvro friendsDrinksAvro) {
        StreamsBuilder builder = new StreamsBuilder();
        final String friendsDrinksApiTopicName = envProps.getProperty("friendsdrinks_api.topic.name");

        KStream<FriendsDrinksId, FriendsDrinksApi> friendsDrinks = builder.stream(friendsDrinksApiTopicName,
                Consumed.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksApiSerde()));

        Predicate<FriendsDrinksId, FriendsDrinksApi> isCreateFriendsDrinksResponseSuccess = (friendsDrinksId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                        friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS));
        Predicate<FriendsDrinksId, FriendsDrinksApi> isDeleteFriendsDrinksResponseSuccess = (friendsDrinksId, friendsDrinksEvent) ->
                friendsDrinksEvent.getApiType().equals(ApiType.DELETE_FRIENDS_DRINKS_RESPONSE) &&
                        friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS);

        KTable<FriendsDrinksId, Long> friendsDrinksCount = friendsDrinks.filter(((s, friendsDrinksEvent) ->
                isCreateFriendsDrinksResponseSuccess.test(s, friendsDrinksEvent) ||
                        isDeleteFriendsDrinksResponseSuccess.test(s, friendsDrinksEvent)))
                .groupByKey(Grouped.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksApiSerde()))
                .aggregate(
                        () -> 0L,
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE)) {
                                return aggValue + 1;
                            } else if (newValue.getApiType().equals(ApiType.DELETE_FRIENDS_DRINKS_RESPONSE)) {
                                return aggValue - 1;
                            } else {
                                throw new RuntimeException(String.format("Encountered unexpected event type %s",
                                        newValue.getApiType().toString()));
                            }
                        },
                        Materialized.with(friendsDrinksAvro.friendsDrinksIdSerde(), Serdes.Long()));

        KStream<FriendsDrinksId, CreateFriendsDrinksRequest> createRequests = friendsDrinks
                .filter(((s, friendsDrinksEvent) -> friendsDrinksEvent.getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksRequest());

        KStream<FriendsDrinksId, FriendsDrinksApi> createResponses = createRequests.leftJoin(friendsDrinksCount,
                (request, count) -> {
                    CreateFriendsDrinksResponse.Builder response = CreateFriendsDrinksResponse.newBuilder();
                    response.setRequestId(request.getRequestId());
                    if (count == null || count < 5) {
                        response.setResult(Result.SUCCESS);
                    } else {
                        response.setResult(Result.FAIL);
                    }
                    FriendsDrinksApi event = FriendsDrinksApi.newBuilder()
                            .setApiType(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE)
                            .setCreateFriendsDrinksResponse(response.build())
                            .build();
                    return event;
                },
                Joined.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.createFriendsDrinksRequestSerde(), Serdes.Long()));

        createResponses.to(friendsDrinksApiTopicName,
                Produced.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksApiSerde()));

        // For now, all delete requests become accepted.
        friendsDrinks.filter(((s, friendsDrinksEvent) ->
                friendsDrinksEvent.getApiType().equals(ApiType.DELETE_FRIENDS_DRINKS_REQUEST)))
                .mapValues((friendsDrinksEvent) -> friendsDrinksEvent.getDeleteFriendsDrinksRequest())
                .mapValues((request) -> FriendsDrinksApi.newBuilder()
                        .setApiType(ApiType.DELETE_FRIENDS_DRINKS_RESPONSE)
                        .setDeleteFriendsDrinksResponse(DeleteFriendsDrinksResponse
                                .newBuilder()
                                .setResult(Result.SUCCESS)
                                .setRequestId(request.getRequestId())
                                .build())
                        .build())
                .to(friendsDrinksApiTopicName,
                        Produced.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksApiSerde()));

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
