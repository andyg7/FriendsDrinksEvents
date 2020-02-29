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
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.UserId;

/**
 * Main FriendsDrinks service.
 */
public class Service {

    public Topology buildTopology(Properties envProps, FriendsDrinksAvro friendsDrinksAvro, UserAvro userAvro) {
        StreamsBuilder builder = new StreamsBuilder();
        final String friendsDrinksTopicName = envProps.getProperty("friendsdrinks.topic.name");

        KStream<UserId, FriendsDrinksEvent> friendsDrinks = builder.stream(friendsDrinksTopicName,
                Consumed.with(userAvro.userIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()));

        Predicate<UserId, FriendsDrinksEvent> isCreateFriendsDrinksResponseSuccess = (userId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                        friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS));
        Predicate<UserId, FriendsDrinksEvent> isDeleteFriendsDrinksResponseSuccess = (userId, friendsDrinksEvent) ->
                friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_RESPONSE) &&
                        friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS);

        KTable<UserId, Long> friendsDrinksCount = friendsDrinks.filter(((s, friendsDrinksEvent) ->
                isCreateFriendsDrinksResponseSuccess.test(s, friendsDrinksEvent) ||
                        isDeleteFriendsDrinksResponseSuccess.test(s, friendsDrinksEvent)))
                .groupByKey(Grouped.with(userAvro.userIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()))
                .aggregate(
                        () -> 0L,
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE)) {
                                return aggValue + 1;
                            } else if (newValue.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_RESPONSE)) {
                                return aggValue - 1;
                            } else {
                                throw new RuntimeException(String.format("Encountered unexpected event type %s",
                                        newValue.getEventType().toString()));
                            }
                        },
                        Materialized.with(userAvro.userIdSerde(), Serdes.Long()));

        KStream<UserId, CreateFriendsDrinksRequest> createRequests = friendsDrinks
                .filter(((s, friendsDrinksEvent) -> friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksRequest());

        KStream<UserId, FriendsDrinksEvent> createResponses = createRequests.leftJoin(friendsDrinksCount,
                (request, count) -> {
                    CreateFriendsDrinksResponse.Builder response = CreateFriendsDrinksResponse.newBuilder();
                    response.setRequestId(request.getRequestId());
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
                Joined.with(userAvro.userIdSerde(), friendsDrinksAvro.createFriendsDrinksRequestSerde(), Serdes.Long()));

        createResponses.to(friendsDrinksTopicName,
                Produced.with(userAvro.userIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()));

        // For now, all delete requests become accepted.
        friendsDrinks.filter(((s, friendsDrinksEvent) ->
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
                .to(friendsDrinksTopicName, Produced.with(userAvro.userIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()));
        return builder.build();
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        Service service = new Service();
        Topology topology = service.buildTopology(envProps,
                new FriendsDrinksAvro(envProps.getProperty("schema.registry.url")),
                new UserAvro(envProps.getProperty("schema.registry.url")));
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
