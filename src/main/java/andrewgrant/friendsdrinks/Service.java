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
public class Service {

    private Topology buildTopology(Properties envProps, FriendsDrinksAvro avro) {
        StreamsBuilder builder = new StreamsBuilder();
        final String friendsDrinksTopicName = envProps.getProperty("friendsdrinks.topic.name");

        KTable<String, Long> friendsDrinksCount = builder.stream(friendsDrinksTopicName,
                Consumed.with(Serdes.String(), avro.createFriendsDrinksEventSerde()))
                .filter(((s, friendsDrinksEvent) ->
                        friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_RESPONSE) &&
                                friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksResponse())
                .groupByKey()
                .count();

        KStream<String, CreateFriendsDrinksRequest> requests = builder.stream(friendsDrinksTopicName,
                Consumed.with(Serdes.String(), avro.createFriendsDrinksEventSerde()))
                .filter(((s, friendsDrinksEvent) -> friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDS_DRINKS_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getCreateFriendsDrinksRequest());

        KStream<String, FriendsDrinksEvent> responses = requests.leftJoin(friendsDrinksCount,
                (request, count) -> {
                    CreateFriendsDrinksResponse.Builder response = CreateFriendsDrinksResponse.newBuilder();
                    if (count == null) {
                        response.setResult(Result.SUCCESS);
                    } else if (count < 5) {
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
                Joined.with(Serdes.String(), avro.createFriendsDrinksRequestSerde(), Serdes.Long()));

        responses.to(friendsDrinksTopicName, Produced.with(Serdes.String(), avro.createFriendsDrinksEventSerde()));
        return builder.build();
    }

    private Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.get("friendsdrinks.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.get("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        FriendsDrinksAvro avro = new FriendsDrinksAvro((String) envProps.get("schema.registry.url"));
        Service service = new Service();
        Topology topology = service.buildTopology(envProps, avro);
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
