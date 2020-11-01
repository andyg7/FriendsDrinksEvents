package andrewgrant.friendsdrinks.membership;

import static andrewgrant.friendsdrinks.TopicNameConfigKey.FRIENDSDRINKS_STATE;
import static andrewgrant.friendsdrinks.env.Properties.load;
import static andrewgrant.friendsdrinks.frontend.TopicNameConfigKey.FRIENDSDRINKS_API;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksInvitation;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksInvitationId;

/**
 * Owns writing to friendsdrinks-invitation topic.
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
                Consumed.with(Serdes.String(), frontendAvroBuilder.apiSerde()));
        KStream<String, FriendsDrinksInvitationResponse> invitationResponses = streamOfSuccessfulInvitationResponses(apiEvents);
        KStream<String, FriendsDrinksInvitationRequest> invitationRequests = streamOfInvitationRequests(apiEvents);

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty(FRIENDSDRINKS_STATE),
                        Consumed.with(friendsDrinksAvroBuilder.friendsDrinksIdSerde(), friendsDrinksAvroBuilder.friendsDrinksStateSerde()));

        streamOfPendingInvitations(invitationResponses, invitationRequests, friendsDrinksStateKTable)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION),
                        Produced.with(avroBuilder.friendsDrinksInvitationIdSerde(),
                                avroBuilder.friendsDrinksInvitationSerde()));

        KStream<String, FriendsDrinksInvitationReplyResponse> invitationReplyResponses =
                streamOfSuccessfulInvitationReplyResponses(apiEvents);
        KStream<String, FriendsDrinksInvitationReplyRequest> invitationReplyRequests =
                streamOfInvitationReplyRequests(apiEvents);
        streamOfResolvedInvitations(invitationReplyResponses, invitationReplyRequests)
                .to(envProps.getProperty(TopicNameConfigKey.FRIENDSDRINKS_INVITATION),
                        Produced.with(avroBuilder.friendsDrinksInvitationIdSerde(),
                                avroBuilder.friendsDrinksInvitationSerde()));

        return builder.build();
    }

    private KStream<FriendsDrinksInvitationId, FriendsDrinksInvitation> streamOfResolvedInvitations(
            KStream<String, FriendsDrinksInvitationReplyResponse> invitationReplyResponses,
            KStream<String, FriendsDrinksInvitationReplyRequest> invitationReplyRequests) {

        return invitationReplyResponses.leftJoin(invitationReplyRequests,
                (l, r) -> r,
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(),
                        frontendAvroBuilder.friendsDrinksInvitationReplyResponseSerde(),
                        frontendAvroBuilder.friendsDrinksInvitationReplyRequestSerde()))
                .map((k, v) -> {
                    FriendsDrinksInvitationId id = FriendsDrinksInvitationId
                            .newBuilder()
                            .setFriendsDrinksId(andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId
                                    .newBuilder()
                                    .setAdminUserId(v.getFriendsDrinksId().getAdminUserId())
                                    .setUuid(v.getFriendsDrinksId().getUuid())
                                    .build()
                            )
                            .setUserId(andrewgrant.friendsdrinks.membership.avro.UserId
                                    .newBuilder()
                                    .setUserId(v.getUserId().getUserId())
                                    .build())
                            .build();
                    return KeyValue.pair(id, null);
                });
    }

    private KStream<FriendsDrinksInvitationId, FriendsDrinksInvitation> streamOfPendingInvitations(
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
                            .setAdminUserId(request.getFriendsDrinksId().getAdminUserId())
                            .setUuid(request.getFriendsDrinksId().getUuid())
                            .build();
                    return KeyValue.pair(friendsDrinksId, request);
                })
                .leftJoin(friendsDrinksStateKTable,
                        (request, state) -> {
                            if (state != null) {
                                return FriendsDrinksInvitation
                                        .newBuilder()
                                        .setFriendsDrinksId(
                                                andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId
                                                        .newBuilder()
                                                        .setUuid(request.getFriendsDrinksId().getUuid())
                                                        .setAdminUserId(request.getFriendsDrinksId().getAdminUserId())
                                                        .build())
                                        .setUserId(
                                                andrewgrant.friendsdrinks.membership.avro.UserId
                                                        .newBuilder()
                                                        .setUserId(request.getUserId().getUserId())
                                                        .build())
                                        .setInvitationId(
                                                FriendsDrinksInvitationId
                                                        .newBuilder()
                                                        .setFriendsDrinksId(
                                                                andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId
                                                                        .newBuilder()
                                                                        .setUuid(request.getFriendsDrinksId().getUuid())
                                                                        .setAdminUserId(request.getFriendsDrinksId().getAdminUserId())
                                                                        .build())
                                                        .setUserId(
                                                                andrewgrant.friendsdrinks.membership.avro.UserId
                                                                        .newBuilder()
                                                                        .setUserId(request.getUserId().getUserId())
                                                                        .build())
                                                        .build())
                                        .setMessage(String.format("Want to join %s?!", state.getName()))
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
                .selectKey((key, value) -> value.getInvitationId());
    }

    private KStream<String, FriendsDrinksInvitationResponse> streamOfSuccessfulInvitationResponses(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getEventType().equals(ApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE) &&
                        friendsDrinksEvent.getFriendsDrinksInvitationResponse().getResult().equals(Result.SUCCESS)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getFriendsDrinksInvitationResponse());
    }

    private KStream<String, FriendsDrinksInvitationReplyResponse> streamOfSuccessfulInvitationReplyResponses(
            KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((friendsDrinksId, friendsDrinksEvent) ->
                (friendsDrinksEvent.getEventType().equals(ApiEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE) &&
                        friendsDrinksEvent.getFriendsDrinksInvitationReplyResponse().getResult().equals(Result.SUCCESS)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getFriendsDrinksInvitationReplyResponse());
    }

    private KStream<String, FriendsDrinksInvitationRequest> streamOfInvitationRequests(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((k, v) -> (v.getEventType().equals(ApiEventType.FRIENDSDRINKS_INVITATION_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getFriendsDrinksInvitationRequest());
    }

    private KStream<String, FriendsDrinksInvitationReplyRequest> streamOfInvitationReplyRequests(KStream<String, ApiEvent> apiEvents) {
        return apiEvents.filter((k, v) -> (v.getEventType().equals(ApiEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST)))
                .mapValues(friendsDrinksEvent -> friendsDrinksEvent.getFriendsDrinksInvitationReplyRequest());
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
