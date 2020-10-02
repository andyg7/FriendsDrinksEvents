package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.api.avro.EventType;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.avro.*;

/**
 * Owns writing to non-API topics.
 */
public class WriterService {

    private static final Logger log = LoggerFactory.getLogger(WriterService.class);

    public Topology buildTopology(Properties envProps,
                                  FriendsDrinksAvro avro) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, FriendsDrinksEvent> apiEvents = builder.stream(envProps.getProperty("friendsdrinks-api.topic.name"),
                Consumed.with(Serdes.String(), avro.apiFriendsDrinksSerde()));
        KStream<String, FriendsDrinksEvent> successApiResponses = streamOfResponses(apiEvents);
        KStream<String, FriendsDrinksEvent> apiRequests = streamOfRequests(apiEvents);

        successApiResponses.join(apiRequests,
                (l, r) -> new EventEmitter().emit(r),
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        avro.apiFriendsDrinksSerde(),
                        avro.apiFriendsDrinksSerde()))
                .selectKey((k, v) -> v.getFriendsDrinksId())
                .to(envProps.getProperty("friendsdrinks-event.topic.name"),
                        Produced.with(avro.friendsDrinksIdSerde(), avro.friendsDrinksEventSerde()));

        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, andrewgrant.friendsdrinks.avro.FriendsDrinksState> friendsDrinksStateStream =
                builder.stream(envProps.getProperty("friendsdrinks-event.topic.name"),
                        Consumed.with(avro.friendsDrinksIdSerde(), avro.friendsDrinksEventSerde()))
                        .groupByKey(Grouped.with(avro.friendsDrinksIdSerde(), avro.friendsDrinksEventSerde()))
                        .aggregate(
                                () -> FriendsDrinksStateAggregate.newBuilder().build(),
                                (aggKey, newValue, aggValue) -> new StateAggregator().handleNewEvent(aggKey, newValue, aggValue),
                                Materialized.<
                                        andrewgrant.friendsdrinks.avro.FriendsDrinksId,
                                        FriendsDrinksStateAggregate, KeyValueStore<Bytes, byte[]>>
                                        as("internal_writer_service_friendsdrinks-state_tracker")
                                        .withKeySerde(avro.friendsDrinksIdSerde())
                                        .withValueSerde(avro.friendsDrinksStateAggregateSerde())
                        ).toStream().mapValues(value -> {
                    if (value == null) {
                        return null;
                    }
                    return value.getFriendsDrinksState();
                });

        friendsDrinksStateStream.to(envProps.getProperty("friendsdrinks-state.topic.name"),
                Produced.with(avro.friendsDrinksIdSerde(), avro.friendsDrinksStateSerde()));

        return builder.build();
    }

    private KStream<String, FriendsDrinksEvent> streamOfResponses(KStream<String, FriendsDrinksEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_RESPONSE) &&
                        friendsDrinksEvent.getCreateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_RESPONSE) &&
                                friendsDrinksEvent.getUpdateFriendsDrinksResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) &&
                                friendsDrinksEvent.getCreateFriendsDrinksInvitationReplyResponse().getResult().equals(Result.SUCCESS)) ||
                        (friendsDrinksEvent.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_RESPONSE) &&
                                friendsDrinksEvent.getDeleteFriendsDrinksResponse().getResult().equals(Result.SUCCESS))
        );
    }

    private KStream<String, FriendsDrinksEvent> streamOfRequests(KStream<String, FriendsDrinksEvent> apiEvents) {
        return apiEvents.filter((k, v) -> v.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_REQUEST) ||
                v.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_REQUEST) ||
                (v.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_INVITATION_REPLY_REQUEST)
                        && v.getCreateFriendsDrinksInvitationReplyRequest().getReply().equals(Reply.ACCEPTED)) ||
                v.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_REQUEST));
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-writer.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
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
