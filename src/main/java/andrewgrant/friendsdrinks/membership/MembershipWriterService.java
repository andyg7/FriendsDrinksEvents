package andrewgrant.friendsdrinks.membership;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.membership.avro.*;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipEvent;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId;
import andrewgrant.friendsdrinks.membership.avro.Status;
import andrewgrant.friendsdrinks.membership.avro.UserId;

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

        KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> friendsDrinksInvitationEventKStream =
                builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION_EVENT),
                        Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(),
                                avroBuilder.friendsDrinksInvitationEventSerde()));
        friendsDrinksInvitationEventKStream
                .filter((k, v) -> v.getEventType().equals(InvitationEventType.RESPONDED_TO) &&
                        v.getFriendsDrinksInvitationRespondedTo().getResponse().equals(Response.ACCEPTED))
                .mapValues(v -> v.getFriendsDrinksInvitationRespondedTo())
                .mapValues(v -> FriendsDrinksMembershipEvent
                        .newBuilder()
                        .setRequestId(v.getRequestId())
                        .setEventType(EventType.ADDED)
                        .setMembershipId(v.getMembershipId())
                        .setFriendsDrinksMembershipAdded(FriendsDrinksMembershipAdded
                                .newBuilder()
                                .setMembershipId(v.getMembershipId())
                                .build())
                        .build())
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));


        KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> friendsDrinksMembershipEventKStream
                = builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_EVENT),
                Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipEventSerdes()));

        buildMembershipStateKTable(friendsDrinksMembershipEventKStream)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_STATE),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()));

        KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> membershipStateKTable =
                builder.table(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_STATE),
                        Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()));

        buildMembershipIdListKeyedByFriendsDrinksIdView(membershipStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_KEYED_BY_FRIENDSDRINKS_ID_STATE),
                        Produced.with(avroBuilder.friendsDrinksIdSerdes(), avroBuilder.friendsDrinksMembershipIdListSerdes()));

        buildMembershipIdListKeyedByUserIdView(membershipStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_MEMBERSHIP_KEYED_BY_USER_ID_STATE),
                        Produced.with(avroBuilder.userIdSerdes(), avroBuilder.friendsDrinksMembershipIdListSerdes()));

        return builder.build();
    }

    private KStream<FriendsDrinksId, FriendsDrinksMembershipIdList> buildMembershipIdListKeyedByFriendsDrinksIdView(
            KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> friendsDrinksMembershipStateKTable) {
        return friendsDrinksMembershipStateKTable.mapValues(v -> {
            if (v.getStatus().equals(Status.REMOVED)) {
                return null;
            } else {
                return v;
            }
        }).groupBy((key, value) ->
                        KeyValue.pair(value.getMembershipId().getFriendsDrinksId(), value),
                Grouped.with(avroBuilder.friendsDrinksIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()))
                .aggregate(
                        () -> FriendsDrinksMembershipIdList
                                .newBuilder()
                                .setIds(new ArrayList<>())
                                .build(),
                        (aggKey, newValue, aggValue) -> {
                            List<FriendsDrinksMembershipId> ids = aggValue.getIds();
                            ids.add(newValue.getMembershipId());
                            FriendsDrinksMembershipIdList idList = FriendsDrinksMembershipIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<FriendsDrinksMembershipId> ids = aggValue.getIds();
                            for (int i = 0; i < ids.size(); i++) {
                                if (ids.get(i).equals(oldValue.getMembershipId())) {
                                    ids.remove(i);
                                    break;
                                }
                            }
                            FriendsDrinksMembershipIdList idList = FriendsDrinksMembershipIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        Materialized.<FriendsDrinksId, FriendsDrinksMembershipIdList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-membership-id-list-keyed-by-friendsdrinks-id-state-store")
                                .withKeySerde(avroBuilder.friendsDrinksIdSerdes())
                                .withValueSerde(avroBuilder.friendsDrinksMembershipIdListSerdes())
                )
                .toStream();
    }

    private KStream<UserId, FriendsDrinksMembershipIdList> buildMembershipIdListKeyedByUserIdView(
            KTable<FriendsDrinksMembershipId, FriendsDrinksMembershipState> friendsDrinksMembershipStateKTable) {

        return friendsDrinksMembershipStateKTable.mapValues(v -> {
            if (v.getStatus().equals(Status.REMOVED)) {
                return null;
            } else {
                return v;
            }
        }).groupBy((key, value) ->
                        KeyValue.pair(value.getMembershipId().getUserId(), value),
                Grouped.with(avroBuilder.userIdSerdes(), avroBuilder.friendsDrinksMembershipStateSerdes()))
                .aggregate(
                        () -> FriendsDrinksMembershipIdList
                                .newBuilder()
                                .setIds(new ArrayList<>())
                                .build(),
                        (aggKey, newValue, aggValue) -> {
                            List<FriendsDrinksMembershipId> ids = aggValue.getIds();
                            ids.add(newValue.getMembershipId());
                            FriendsDrinksMembershipIdList idList = FriendsDrinksMembershipIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        (aggKey, oldValue, aggValue) -> {
                            List<FriendsDrinksMembershipId> ids = aggValue.getIds();
                            for (int i = 0; i < ids.size(); i++) {
                                if (ids.get(i).equals(oldValue.getMembershipId())) {
                                    ids.remove(i);
                                    break;
                                }
                            }
                            if (ids.size() == 0) {
                                return null;
                            }
                            FriendsDrinksMembershipIdList idList = FriendsDrinksMembershipIdList
                                    .newBuilder(aggValue)
                                    .setIds(ids)
                                    .build();
                            return idList;
                        },
                        Materialized.<UserId, FriendsDrinksMembershipIdList, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-membership-id-list-keyed-by-user-id-state-store")
                                .withKeySerde(avroBuilder.userIdSerdes())
                                .withValueSerde(avroBuilder.friendsDrinksMembershipIdListSerdes())
                )
                .toStream();
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipState> buildMembershipStateKTable(
            KStream<FriendsDrinksMembershipId, FriendsDrinksMembershipEvent> membershipEventKStream) {
        return membershipEventKStream.groupByKey(Grouped.with(
                avroBuilder.friendsDrinksMembershipIdSerdes(),
                avroBuilder.friendsDrinksMembershipEventSerdes()))
                .aggregate(
                        () -> FriendsDrinksMembershipStateAggregate.newBuilder().build(),
                        (aggKey, newValue, aggValue) -> new StateAggregator().handleNewEvent(aggKey, newValue, aggValue),
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
        log.info("Starting MembershipWriterService application...");

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
