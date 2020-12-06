package andrewgrant.friendsdrinks.membership;

import static andrewgrant.friendsdrinks.TopicNameConfigKey.FRIENDSDRINKS_STATE;
import static andrewgrant.friendsdrinks.env.Properties.load;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.avro.*;

/**
 * Owns writing to friendsdrinks-invitation-event topic.
 */
public class InvitationWriterService {

    private static final Logger log = LoggerFactory.getLogger(InvitationWriterService.class);

    private Properties envProps;
    private andrewgrant.friendsdrinks.membership.AvroBuilder avroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder;
    private andrewgrant.friendsdrinks.AvroBuilder friendsDrinksAvroBuilder;

    public InvitationWriterService(Properties envProps, AvroBuilder avroBuilder,
                                   andrewgrant.friendsdrinks.frontend.AvroBuilder frontendAvroBuilder,
                                   andrewgrant.friendsdrinks.AvroBuilder friendsDrinksAvroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
        this.frontendAvroBuilder = frontendAvroBuilder;
        this.friendsDrinksAvroBuilder = friendsDrinksAvroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ApiEvent> apiEvents = builder.stream(envProps.getProperty(FRIENDSDRINKS_API),
                Consumed.with(Serdes.String(), frontendAvroBuilder.apiEventSerde()));
        KStream<String, FriendsDrinksInvitationResponse> invitationResponses = invitationResponses(apiEvents);
        KStream<String, FriendsDrinksInvitationRequest> invitationRequests = invitationRequests(apiEvents);

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty(FRIENDSDRINKS_STATE),
                        Consumed.with(friendsDrinksAvroBuilder.friendsDrinksIdSerde(), friendsDrinksAvroBuilder.friendsDrinksStateSerde()));
        invitationEvents(invitationResponses, invitationRequests, friendsDrinksStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION_EVENT),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(),
                                avroBuilder.friendsDrinksInvitationEventSerde()));

        KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> invitationRepliedEvents =
                invitationRepliedEvents(
                        streamOfSuccessfulInvitationReplyResponses(apiEvents),
                        streamOfInvitationReplyRequests(apiEvents));
        invitationRepliedEvents.to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION_EVENT),
                Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(),
                        avroBuilder.friendsDrinksInvitationEventSerde()));

        // Build invitation state.
        friendsDrinksInvitationState(builder.stream(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION_EVENT),
                Consumed.with(avroBuilder.friendsDrinksMembershipIdSerdes(),
                        avroBuilder.friendsDrinksInvitationEventSerde())))
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION_STATE),
                        Produced.with(avroBuilder.friendsDrinksMembershipIdSerdes(),
                                avroBuilder.friendsDrinksInvitationStateSerde()));

        return builder.build();
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationState> friendsDrinksInvitationState(
            KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> friendsDrinksInvitationEventKStream) {
        return friendsDrinksInvitationEventKStream
                .groupByKey(Grouped.with(avroBuilder.friendsDrinksMembershipIdSerdes(), avroBuilder.friendsDrinksInvitationEventSerde()))
                .aggregate(
                        () -> FriendsDrinksInvitationStateAggregate.newBuilder().build(),
                        (aggKey, newValue, aggValue) -> new InvitationStateAggregator().handleNewEvent(aggKey, newValue, aggValue),
                        Materialized.<
                                FriendsDrinksMembershipId,
                                FriendsDrinksInvitationStateAggregate, KeyValueStore<Bytes, byte[]>>
                                as("friendsdrinks-invitation-state-aggregate-state-store")
                                .withKeySerde(avroBuilder.friendsDrinksMembershipIdSerdes())
                                .withValueSerde(avroBuilder.friendsDrinksInvitationStateAggregateSerde())
                ).toStream().mapValues(v -> {
                    if (v == null) {
                        return null;
                    }
                    return v.getFriendsDrinksInvitationState();
                });
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> invitationRepliedEvents(
            KStream<String, FriendsDrinksInvitationReplyResponse> invitationReplyResponses,
            KStream<String, FriendsDrinksInvitationReplyRequest> invitationReplyRequests) {

        return invitationReplyResponses.leftJoin(invitationReplyRequests,
                (l, r) -> r,
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        frontendAvroBuilder.friendsDrinksInvitationReplyResponseSerde(),
                        frontendAvroBuilder.friendsDrinksInvitationReplyRequestSerde()))
                .map((k, v) -> {
                    FriendsDrinksMembershipId id = FriendsDrinksMembershipId
                            .newBuilder()
                            .setFriendsDrinksId(FriendsDrinksId
                                    .newBuilder()
                                    .setUuid(v.getMembershipId().getFriendsDrinksId().getUuid())
                                    .build()
                            )
                            .setUserId(UserId
                                    .newBuilder()
                                    .setUserId(v.getMembershipId().getUserId().getUserId())
                                    .build())
                            .build();
                    FriendsDrinksInvitationAnswer answer;
                    if (v.getReply().equals(FriendsDrinksInvitationReply.ACCEPTED)) {
                        answer = FriendsDrinksInvitationAnswer.ACCEPTED;
                    } else {
                        answer = FriendsDrinksInvitationAnswer.REJECTED;
                    }
                    return KeyValue.pair(id, FriendsDrinksInvitationEvent
                            .newBuilder()
                            .setEventType(InvitationEventType.RESPONDED_TO)
                            .setMembershipId(id)
                            .setRequestId(v.getRequestId())
                            .setFriendsDrinksInvitationRespondedTo(FriendsDrinksInvitationRespondedTo
                                    .newBuilder()
                                    .setMembershipId(id)
                                    .setRequestId(v.getRequestId())
                                    .setAnswer(answer)
                                    .build())
                            .build());
                });
    }

    private KStream<FriendsDrinksMembershipId, FriendsDrinksInvitationEvent> invitationEvents(
            KStream<String, FriendsDrinksInvitationResponse> invitationResponses,
            KStream<String, FriendsDrinksInvitationRequest> invitationRequests,
            KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable) {

        return invitationResponses.join(invitationRequests,
                (l, r) -> r,
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        frontendAvroBuilder.friendsDrinksInvitationResponseSerde(),
                        frontendAvroBuilder.friendsDrinksInvitationRequestSerde()))
                .map((s, request) -> {
                    FriendsDrinksId friendsDrinksId = FriendsDrinksId
                            .newBuilder()
                            .setUuid(request.getMembershipId().getFriendsDrinksId().getUuid())
                            .build();
                    return KeyValue.pair(friendsDrinksId, request);
                })
                .leftJoin(friendsDrinksStateKTable,
                        (request, state) -> {
                            if (state != null && (!state.getStatus().equals(FriendsDrinksStatus.DELETED))) {
                                FriendsDrinksMembershipId friendsDrinksMembershipId = FriendsDrinksMembershipId.newBuilder()
                                        .setFriendsDrinksId(
                                                FriendsDrinksId.newBuilder()
                                                        .setUuid(request.getMembershipId().getFriendsDrinksId().getUuid())
                                                        .build())
                                        .setUserId(
                                                UserId.newBuilder()
                                                        .setUserId(request.getMembershipId().getUserId().getUserId())
                                                        .build())
                                        .build();
                                return FriendsDrinksInvitationEvent
                                        .newBuilder()
                                        .setEventType(InvitationEventType.CREATED)
                                        .setRequestId(request.getRequestId())
                                        .setMembershipId(friendsDrinksMembershipId)
                                        .setFriendsDrinksInvitationCreated(FriendsDrinksInvitationCreated
                                                .newBuilder()
                                                .setMessage(String.format("Want to join %s?!", state.getName()))
                                                .setMembershipId(friendsDrinksMembershipId)
                                                .setRequestId(request.getRequestId())
                                                .build())
                                        .build();
                            } else {
                                throw new RuntimeException(String.format("Failed to find FriendsDrinks state %s",
                                        request.getRequestId()));
                            }
                        },
                        Joined.with(friendsDrinksAvroBuilder.friendsDrinksIdSerde(),
                                frontendAvroBuilder.friendsDrinksInvitationRequestSerde(),
                                friendsDrinksAvroBuilder.friendsDrinksStateSerde())
                )
                .selectKey((key, value) -> value.getMembershipId());
    }

    private KStream<String, FriendsDrinksInvitationResponse> invitationResponses(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) -> friendsDrinksEvent.getEventType()
                .equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT) &&
                (friendsDrinksEvent.getFriendsDrinksMembershipEvent().getEventType()
                        .equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE) &&
                        friendsDrinksEvent.getFriendsDrinksMembershipEvent()
                                .getFriendsDrinksInvitationResponse().getResult().equals(Result.SUCCESS)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getFriendsDrinksMembershipEvent().getFriendsDrinksInvitationResponse());
    }

    private KStream<String, FriendsDrinksInvitationReplyResponse> streamOfSuccessfulInvitationReplyResponses(
            KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) -> friendsDrinksEvent.getEventType()
                .equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT) &&
                (friendsDrinksEvent.getFriendsDrinksMembershipEvent().getEventType()
                        .equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) &&
                        friendsDrinksEvent.getFriendsDrinksMembershipEvent()
                                .getFriendsDrinksInvitationReplyResponse().getResult().equals(Result.SUCCESS)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getFriendsDrinksMembershipEvent()
                        .getFriendsDrinksInvitationReplyResponse());
    }

    private KStream<String, FriendsDrinksInvitationRequest> invitationRequests(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((k, v) -> v.getEventType().equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT) &&
                v.getFriendsDrinksMembershipEvent().getEventType()
                        .equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REQUEST))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getFriendsDrinksMembershipEvent()
                        .getFriendsDrinksInvitationRequest());
    }

    private KStream<String, FriendsDrinksInvitationReplyRequest> streamOfInvitationReplyRequests(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((k, v) -> v.getEventType().equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT) &&
                v.getFriendsDrinksMembershipEvent().getEventType()
                        .equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getFriendsDrinksMembershipEvent()
                        .getFriendsDrinksInvitationReplyRequest());
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("friendsdrinks-invitation-writer.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return streamProps;
    }

    public static void main(String args[]) throws IOException  {
        Properties envProps = load(args[0]);
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");

        InvitationWriterService writerService = new InvitationWriterService(
                envProps,
                new andrewgrant.friendsdrinks.membership.AvroBuilder(schemaRegistryUrl),
                new andrewgrant.friendsdrinks.frontend.AvroBuilder(schemaRegistryUrl),
                new andrewgrant.friendsdrinks.AvroBuilder(schemaRegistryUrl));

        Topology topology = writerService.buildTopology();
        Properties streamProps = writerService.buildStreamsProperties(envProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);
        log.info("Starting InvitationWriterService application...");

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
