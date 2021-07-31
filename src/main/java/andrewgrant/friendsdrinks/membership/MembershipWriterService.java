package andrewgrant.friendsdrinks.membership;

import static andrewgrant.friendsdrinks.streamsconfig.FilePropsLoader.load;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.streamsconfig.SharedConfigSetter;

import com.sun.net.httpserver.HttpServer;

/**
 * Owns writing to friendsdrinks-membership-event.
 */
public class MembershipWriterService {

    private static final Logger log = LoggerFactory.getLogger(MembershipWriterService.class);

    private Properties envProps;
    private AvroBuilder avroBuilder;

    public MembershipWriterService(Properties envProps,
                                   AvroBuilder avroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION_EVENT),
                Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(),
                        avroBuilder.friendsDrinksInvitationEventSerde()))
                .filter((k, v) -> v.getEventType().equals(InvitationEventType.RESPONDED_TO) &&
                        v.getFriendsDrinksInvitationRespondedTo().getAnswer().equals(FriendsDrinksInvitationAnswer.ACCEPTED))
                .mapValues(v -> v.getFriendsDrinksInvitationRespondedTo())
                .mapValues(v -> FriendsDrinksMembershipEvent
                        .newBuilder()
                        .setRequestId(v.getRequestId())
                        .setEventType(FriendsDrinksMembershipEventType.ADDED)
                        .setMembershipId(v.getMembershipId())
                        .setFriendsDrinksMembershipAdded(FriendsDrinksMembershipAdded
                                .newBuilder()
                                .setMembershipId(v.getMembershipId())
                                .build())
                        .build())
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));


        buildMembershipState(
                builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                        Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes())))
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_STATE),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()));

        return builder.build();
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipState> buildMembershipState(
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> membershipEventKStream) {
        return membershipEventKStream.groupByKey(Grouped.with(
                avroBuilder.friendsDrinksMembershipIdSerdes(),
                avroBuilder.friendsDrinksMembershipEventSerdes()))
                .aggregate(
                        () -> FriendsDrinksMembershipStateAggregate.newBuilder().build(),
                        (aggKey, newValue, aggValue) -> new MembershipStateAggregator().handleNewEvent(aggKey, newValue, aggValue),
                        Materialized.<
                                FriendsDrinksMembershipId,
                                FriendsDrinksMembershipStateAggregate, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-membership-state-aggregate-state-store")
                                .withKeySerde(avroBuilder.friendsDrinksMembershipIdSerdes())
                                .withValueSerde(avroBuilder.friendsDrinksMembershipStateAggregateSerdes())
                ).toStream().mapValues(v -> v.getFriendsDrinksMembershipState());
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("friendsdrinks-membership-writer.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        if (envProps.getProperty("streams.dir") != null) {
            streamProps.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("streams.dir"));
        }
        streamProps = SharedConfigSetter.addSharedConfig(streamProps);
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        MembershipWriterService writerService = new MembershipWriterService(
                envProps, new AvroBuilder(schemaRegistryUrl));
        Topology topology = writerService.buildTopology();
        Properties streamProps = writerService.buildStreamsProperties(envProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);
        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception {}", exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        log.info("Starting MembershipWriterService application...");

        HttpServer healthCheckServer = andrewgrant.friendsdrinks.health.Server.buildServer(8080, kafkaStreams);

        final CountDownLatch latch = new CountDownLatch(2);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread("health-check-shutdown-hook") {
            @Override
            public void run() {
                andrewgrant.friendsdrinks.health.Server.stop(healthCheckServer);
                latch.countDown();
            }
        });

        kafkaStreams.start();
        andrewgrant.friendsdrinks.health.Server.start(healthCheckServer);
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
