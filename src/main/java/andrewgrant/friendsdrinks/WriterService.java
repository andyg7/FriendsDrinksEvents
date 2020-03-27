package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.UserId;

/**
 * Owns writing to friendsdrinkfriendsdrinks.
 */
public class WriterService {

    public Topology buildTopology(Properties envProps,
                                  UserAvro userAvro,
                                  FriendsDrinksAvro friendsDrinksAvro) {
        StreamsBuilder builder = new StreamsBuilder();
        String friendsDrinksApiTopicName = envProps.getProperty("friendsdrinks_api.topic.name");

        KStream<UserId, FriendsDrinksApi> friendsDrinksEventKStream = builder.stream(friendsDrinksApiTopicName,
                Consumed.with(userAvro.userIdSerde(), friendsDrinksAvro.friendsDrinksApiSerde()));

        KStream<String, FriendsDrinksApi> responses = friendsDrinksEventKStream
                .filter((userId, friendsDrinksEvent) ->
                        (friendsDrinksEvent.getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                                friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                                (friendsDrinksEvent.getApiType().equals(ApiType.DELETE_FRIENDS_DRINKS_RESPONSE) &&
                                        friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS))
                )
                .selectKey((k, v) -> {
                    if (v.getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_RESPONSE)) {
                        return v.getCreateFriendsDrinksResponse().getRequestId();
                    } else if (v.getApiType().equals(ApiType.DELETE_FRIENDS_DRINKS_RESPONSE)) {
                        return v.getDeleteFriendsDrinksResponse().getRequestId();
                    } else {
                        throw new RuntimeException(
                                String.format("Received unexpected event type %s", v.getApiType().toString()));
                    }
                });

        KStream<String, FriendsDrinksApi> requests = friendsDrinksEventKStream
                .filter((k, v) -> v.getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_REQUEST) ||
                        v.getApiType().equals(ApiType.DELETE_FRIENDS_DRINKS_REQUEST))
                .selectKey(((k, v) -> {
                    if (v.getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)) {
                        return v.getCreateFriendsDrinksRequest().getRequestId();
                    } else if (v.getApiType().equals(ApiType.DELETE_FRIENDS_DRINKS_REQUEST)) {
                        return v.getDeleteFriendsDrinksRequest().getRequestId();
                    } else {
                        throw new RuntimeException(
                                String.format("Received unexpected event type %s", v.getApiType().toString()));
                    }
                }));

        responses.join(requests,
                (l, r) -> {
                    if (r.getApiType().equals(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)) {
                        CreateFriendsDrinksRequest createFriendsDrinksRequest =
                                r.getCreateFriendsDrinksRequest();
                        return FriendsDrinksEvent.newBuilder()
                                .setEventType(EventType.CREATED)
                                .setAdminUser(createFriendsDrinksRequest.getAdminUserId())
                                .setUserIds(createFriendsDrinksRequest.getUserIds().stream().collect(Collectors.toList()))
                                .setScheduleType(createFriendsDrinksRequest.getScheduleType())
                                .setCronSchedule(createFriendsDrinksRequest.getCronSchedule())
                                .build();
                    } else if (r.getApiType().equals(ApiType.DELETE_FRIENDS_DRINKS_REQUEST)) {
                        return null;
                    } else {
                        throw new RuntimeException(
                                String.format("Received unexpected event type %s", r.getApiType().toString()));
                    }
                },
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        friendsDrinksAvro.friendsDrinksApiSerde(),
                        friendsDrinksAvro.friendsDrinksApiSerde()))
                .selectKey((k, v) -> v.getFriendsDrinksId())
                .to(envProps.getProperty("friendsdrinks.topic.name"),
                        Produced.with(Serdes.String(), friendsDrinksAvro.friendsDrinksEventSerde()));

        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks_writer.application"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        WriterService writerService = new WriterService();
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        UserAvro userAvro = new UserAvro(schemaRegistryUrl);
        FriendsDrinksAvro friendsDrinksAvro = new FriendsDrinksAvro(schemaRegistryUrl);
        Topology topology = writerService.buildTopology(envProps, userAvro, friendsDrinksAvro);
        Properties streamProps = writerService.buildStreamsProperties(envProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        kafkaStreams.start();;
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
