package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.CreatedFriendsDrinks;

/**
 * Owns writing to non-API topics.
 */
public class WriterService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);


    public Topology buildTopology(Properties envProps,
                                  FriendsDrinksAvro friendsDrinksAvro) {
        StreamsBuilder builder = new StreamsBuilder();
        String friendsDrinksApiTopicName = envProps.getProperty("friendsdrinks_api.topic.name");

        KStream<FriendsDrinksId, FriendsDrinksEvent> friendsDrinksEventKStream = builder.stream(friendsDrinksApiTopicName,
                Consumed.with(friendsDrinksAvro.apiFriendsDrinksIdSerde(), friendsDrinksAvro.apiFriendsDrinksSerde()));

        KStream<String, FriendsDrinksEvent> successfulResponses = friendsDrinksEventKStream
                .filter((friendsDrinksId, friendsDrinksEvent) ->
                        (friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                                friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                                (friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_RESPONSE) &&
                                        friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS))
                )
                .selectKey((k, v) -> {
                    if (v.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE)) {
                        log.info("Got create response {}", v.getCreateFriendsDrinksResponse().getRequestId());
                        return v.getCreateFriendsDrinksResponse().getRequestId();
                    } else if (v.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_RESPONSE)) {
                        log.info("Got delete response {}", v.getDeleteFriendsDrinksResponse().getRequestId());
                        return v.getDeleteFriendsDrinksResponse().getRequestId();
                    } else {
                        throw new RuntimeException(
                                String.format("Received unexpected event type %s", v.getEventType().toString()));
                    }
                });

        KStream<String, FriendsDrinksEvent> requests = friendsDrinksEventKStream
                .filter((k, v) -> v.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_REQUEST) ||
                        v.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_REQUEST))
                .selectKey(((k, v) -> {
                    if (v.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_REQUEST)) {
                        log.info("Got create request {}", v.getCreateFriendsDrinksRequest().getRequestId());
                        return v.getCreateFriendsDrinksRequest().getRequestId();
                    } else if (v.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_REQUEST)) {
                        log.info("Got delete request {}", v.getDeleteFriendsDrinksRequest().getRequestId());
                        return v.getDeleteFriendsDrinksRequest().getRequestId();
                    } else {
                        throw new RuntimeException(
                                String.format("Received unexpected event type %s", v.getEventType().toString()));
                    }
                }));

        successfulResponses.join(requests,
                (l, r) -> {
                    if (r.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_REQUEST)) {
                        log.info("Got create join {}", r.getCreateFriendsDrinksRequest().getRequestId());
                        CreateFriendsDrinksRequest createFriendsDrinksRequest =
                                r.getCreateFriendsDrinksRequest();
                        CreatedFriendsDrinks friendsDrinks = CreatedFriendsDrinks
                                .newBuilder()
                                .setAdminUserId(createFriendsDrinksRequest.getAdminUserId())
                                .setName(createFriendsDrinksRequest.getName())
                                .setUserIds(createFriendsDrinksRequest.getUserIds().stream().collect(Collectors.toList()))
                                .setScheduleType(andrewgrant.friendsdrinks.avro.ScheduleType.valueOf(
                                        createFriendsDrinksRequest.getScheduleType().toString()))
                                .setCronSchedule(createFriendsDrinksRequest.getCronSchedule())
                                .build();
                        return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent.newBuilder()
                                .setEventType(andrewgrant.friendsdrinks.avro.EventType.CREATED)
                                .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                                        .newBuilder()
                                        .setId(r.getCreateFriendsDrinksRequest().getFriendsDrinksId().getId())
                                        .build())
                                .setCreatedFriendsDrinks(friendsDrinks)
                                .build();
                    } else if (r.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_REQUEST)) {
                        log.info("Got delete join {}", r.getDeleteFriendsDrinksRequest().getRequestId());
                        return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                                .newBuilder()
                                .setEventType(andrewgrant.friendsdrinks.avro.EventType.DELETED)
                                .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                                        .newBuilder()
                                        .setId(r.getCreateFriendsDrinksRequest().getFriendsDrinksId().getId())
                                        .build())
                                .build();
                    } else {
                        throw new RuntimeException(
                                String.format("Received unexpected event type %s", r.getEventType().toString()));
                    }
                },
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        friendsDrinksAvro.apiFriendsDrinksSerde(),
                        friendsDrinksAvro.apiFriendsDrinksSerde()))
                .selectKey((k, v) -> v.getFriendsDrinksId())
                .to(envProps.getProperty("friendsdrinks.topic.name"),
                        Produced.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()));


        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksEvent> friendsDrinksEventStream =
                builder.stream(envProps.getProperty("friendsdrinks.topic.name"),
                        Consumed.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()))
                        .mapValues((value -> {
                            if (value.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.CREATED)) {
                                return value;
                            } else if (value.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.DELETED)) {
                                // Tombstone deleted friends drinks.
                                return null;
                            } else {
                                throw new RuntimeException(String.format("Unknown event type %s", value.getEventType().toString()));
                            }
                        }));

        friendsDrinksEventStream.to(envProps.getProperty("currFriendsdrinks.topic.name"),
                Produced.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()));

        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks_writer.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        WriterService writerService = new WriterService();
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        FriendsDrinksAvro friendsDrinksAvro = new FriendsDrinksAvro(schemaRegistryUrl);
        Topology topology = writerService.buildTopology(envProps, friendsDrinksAvro);
        Properties streamProps = writerService.buildStreamsProperties(envProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);
        log.info("Starting WriterService application...");

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        kafkaStreams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
