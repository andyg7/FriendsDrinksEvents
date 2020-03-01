package andrewgrant.friendsdrinks;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.stream.Collectors;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.UserId;

/**
 * Owns writing to currFriendsdrinks.
 */
public class WriterService {

    public Topology buildTopology(Properties envProps,
                                  UserAvro userAvro,
                                  FriendsDrinksAvro friendsDrinksAvro) {
        StreamsBuilder builder = new StreamsBuilder();
        String friendsDrinksTopicName = envProps.getProperty("friendsdrinks.topic.name");

        KStream<UserId, FriendsDrinksEvent> friendsDrinksEventKStream = builder.stream(friendsDrinksTopicName,
                Consumed.with(userAvro.userIdSerde(), friendsDrinksAvro.friendsDrinksEventSerde()));

        KStream<String, FriendsDrinksEvent> responses = friendsDrinksEventKStream
                .filter((userId, friendsDrinksEvent) ->
                        (friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                                friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                                (friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_RESPONSE) &&
                                        friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS))
                )
                .selectKey((k, v) -> {
                    if (v.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE)) {
                        return v.getCreateFriendsDrinksResponse().getRequestId();
                    } else if (v.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_RESPONSE)) {
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
                        return v.getCreateFriendsDrinksRequest().getRequestId();
                    } else if (v.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_REQUEST)) {
                        return v.getDeleteFriendsDrinksRequest().getRequestId();
                    } else {
                        throw new RuntimeException(
                                String.format("Received unexpected event type %s", v.getEventType().toString()));
                    }
                }));

        responses.join(requests,
                (l, r) -> {
                    if (r.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_REQUEST)) {
                        CreateFriendsDrinksRequest createFriendsDrinksRequest =
                                r.getCreateFriendsDrinksRequest();
                        return FriendsDrinks.newBuilder()
                                .setAdminUser(createFriendsDrinksRequest.getAdminUserId())
                                .setUserIds(createFriendsDrinksRequest.getUserIds().stream().collect(Collectors.toList()))
                                .setScheduleType(createFriendsDrinksRequest.getScheduleType())
                                .setCronSchedule(createFriendsDrinksRequest.getCronSchedule())
                                .build();
                    } else if (r.getEventType().equals(EventType.DELETE_FRIENDS_DRINKS_REQUEST)) {
                        return null;
                    } else {
                        throw new RuntimeException(
                                String.format("Received unexpected event type %s", r.getEventType().toString()));
                    }
                },
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        friendsDrinksAvro.friendsDrinksEventSerde(),
                        friendsDrinksAvro.friendsDrinksEventSerde()))
                .selectKey((k, v) -> v.getFriendsDrinksId())
                .to(envProps.getProperty("currFriendsdrinks.topic.name"),
                        Produced.with(Serdes.String(), friendsDrinksAvro.friendsDrinksSerde()));

        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("currFriendsdrinks.topic.name"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) {

    }
}
