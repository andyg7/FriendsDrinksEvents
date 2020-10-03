package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import andrewgrant.friendsdrinks.api.avro.*;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.UserId;
import andrewgrant.friendsdrinks.user.avro.UserState;


/**
 * Processes invitation requests.
 */
public class InvitationRequestService {

    private static final Logger log = LoggerFactory.getLogger(InvitationRequestService.class);

    public Topology buildTopology(Properties envProps, FriendsDrinksAvro friendsDrinksAvro,
                                  UserAvro userAvro) {

        StreamsBuilder builder = new StreamsBuilder();

        final String apiTopicName = envProps.getProperty("friendsdrinks-api.topic.name");
        KStream<String, FriendsDrinksEvent> apiEvents = builder.stream(apiTopicName,
                Consumed.with(Serdes.String(), friendsDrinksAvro.apiFriendsDrinksSerde()));

        KTable<FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable =
                builder.table(envProps.getProperty("friendsdrinks-state.topic.name"),
                        Consumed.with(friendsDrinksAvro.friendsDrinksIdSerde(), friendsDrinksAvro.friendsDrinksStateSerde()));

        KTable<andrewgrant.friendsdrinks.user.avro.UserId, UserState> userState =
                builder.table(envProps.getProperty("user-state.topic.name"),
                        Consumed.with(userAvro.userIdSerde(), userAvro.userStateSerde()));

        String pendingInvitationsTopicName = envProps.getProperty("friendsdrinks-pending-invitation.topic.name");
        KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations = builder.table(
                pendingInvitationsTopicName,
                Consumed.with(friendsDrinksAvro.friendsDrinksPendingInvitationIdSerde(), friendsDrinksAvro.friendsDrinksPendingInvitationSerde()));
        handleInvitations(
                apiEvents,
                friendsDrinksStateKTable,
                pendingFriendsDrinksInvitations,
                userState,
                friendsDrinksAvro,
                userAvro,
                apiTopicName,
                pendingInvitationsTopicName);

        return builder.build();
    }

    private void handleInvitations(KStream<String, FriendsDrinksEvent> apiEvents,
                                   KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                   KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations,
                                   KTable<andrewgrant.friendsdrinks.user.avro.UserId, UserState> userState,
                                   FriendsDrinksAvro friendsDrinksAvro, UserAvro userAvro, String apiTopicName, String pendingInvitationsTopicName) {

        handleInvitationRequests(apiEvents, friendsDrinksStateKTable, userState, friendsDrinksAvro, userAvro, apiTopicName,
                pendingInvitationsTopicName);
        handleInvitationReplies(apiEvents, pendingFriendsDrinksInvitations, friendsDrinksAvro, apiTopicName,
                pendingInvitationsTopicName);

    }

    private void handleInvitationRequests(KStream<String, FriendsDrinksEvent> apiEvents,
                                          KTable<andrewgrant.friendsdrinks.avro.FriendsDrinksId, FriendsDrinksState> friendsDrinksStateKTable,
                                          KTable<UserId, UserState> userState, FriendsDrinksAvro avro, UserAvro userAvro,
                                          String apiTopicName, String pendingInvitationsTopicName) {

        // FriendsDrinks invitation requests
        KStream<String, CreateFriendsDrinksInvitationRequest> friendsDrinksInvitations = apiEvents
                .filter((key, value) -> value.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_INVITATION_REQUEST))
                .mapValues(value -> value.getCreateFriendsDrinksInvitationRequest());
        KStream<andrewgrant.friendsdrinks.avro.FriendsDrinksId, CreateFriendsDrinksInvitationAggregateResult>
                createFriendsDrinksInvitationAggregateResult = friendsDrinksInvitations.selectKey((key, value) ->
                andrewgrant.friendsdrinks.avro.FriendsDrinksId
                        .newBuilder()
                        .setAdminUserId(value.getFriendsDrinksId().getAdminUserId())
                        .setFriendsDrinksId(value.getFriendsDrinksId().getFriendsDrinksId())
                        .build())
                .leftJoin(friendsDrinksStateKTable,
                        (request, state) -> {
                            CreateFriendsDrinksInvitationAggregateResult aggregateResult =
                                    new CreateFriendsDrinksInvitationAggregateResult();
                            if (state != null) {
                                if (state.getUserIds().contains(request.getUserId()) ||
                                        state.getFriendsDrinksId().getAdminUserId().equals(request.getUserId().getUserId())) {
                                    aggregateResult.failed = true;
                                } else {
                                    // Confirms the FriendsDrinks exists.
                                    aggregateResult.createFriendsDrinksInvitationRequest = request;
                                    aggregateResult.name = state.getName();
                                    aggregateResult.failed = false;
                                }
                            } else {
                                aggregateResult.failed = true;
                            }
                            return aggregateResult;
                        },
                        Joined.with(avro.friendsDrinksIdSerde(), avro.createFriendsDrinksInvitationRequestSerde(), avro.friendsDrinksStateSerde())
                );

        createFriendsDrinksInvitationAggregateResult.filter((key, value) -> value.failed == false).map((key, value) -> {
            FriendsDrinksPendingInvitation pendingInvitation = FriendsDrinksPendingInvitation
                    .newBuilder()
                    .setFriendsDrinksId(value.createFriendsDrinksInvitationRequest.getFriendsDrinksId())
                    .setUserId(value.createFriendsDrinksInvitationRequest.getUserId())
                    .setInvitationId(
                            FriendsDrinksPendingInvitationId
                                    .newBuilder()
                                    .setFriendsDrinksId(value.createFriendsDrinksInvitationRequest.getFriendsDrinksId())
                                    .setUserId(value.createFriendsDrinksInvitationRequest.getUserId())
                                    .build())
                    .setMessage(String.format("Want to join %s?!", value.name))
                    .build();
            return new KeyValue<>(pendingInvitation.getInvitationId(), pendingInvitation);
        }).to(pendingInvitationsTopicName,
                Produced.with(
                        avro.friendsDrinksPendingInvitationIdSerde(),
                        avro.friendsDrinksPendingInvitationSerde()));

        createFriendsDrinksInvitationAggregateResult.map((key, value) -> {
            CreateFriendsDrinksInvitationResponse response;
            if (value.failed) {
                response = CreateFriendsDrinksInvitationResponse
                        .newBuilder()
                        .setRequestId(value.createFriendsDrinksInvitationRequest.getRequestId())
                        .setResult(Result.FAIL)
                        .build();
            } else {
                response = CreateFriendsDrinksInvitationResponse
                        .newBuilder()
                        .setRequestId(value.createFriendsDrinksInvitationRequest.getRequestId())
                        .setResult(Result.SUCCESS)
                        .build();
            }
            FriendsDrinksEvent friendsDrinksEvent = FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(EventType.CREATE_FRIENDSDRINKS_INVITATION_RESPONSE)
                    .setRequestId(value.createFriendsDrinksInvitationRequest.getRequestId())
                    .setCreateFriendsDrinksInvitationResponse(response)
                    .build();
            return new KeyValue<>(
                    friendsDrinksEvent.getCreateFriendsDrinksInvitationResponse().getRequestId(),
                    friendsDrinksEvent);
        }).to(apiTopicName, Produced.with(Serdes.String(), avro.apiFriendsDrinksSerde()));
    }

    private void handleInvitationReplies(KStream<String, FriendsDrinksEvent> apiEvents,
                                         KTable<FriendsDrinksPendingInvitationId, FriendsDrinksPendingInvitation> pendingFriendsDrinksInvitations,
                                         FriendsDrinksAvro avro, String apiTopicName, String pendingInvitationsTopicName) {

        // FriendsDrinks replies
        KStream<String, CreateFriendsDrinksInvitationReplyRequest> createFriendsDrinksInvitationReplyRequests = apiEvents
                .filter((key, value) -> value.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_INVITATION_REPLY_REQUEST))
                .mapValues(value -> value.getCreateFriendsDrinksInvitationReplyRequest());
        KStream<FriendsDrinksPendingInvitationId, CreateFriendsDrinksInvitationReplyResponse> createFriendsDrinksInvitationReplyResponses =
                createFriendsDrinksInvitationReplyRequests.selectKey((key, value) -> FriendsDrinksPendingInvitationId
                        .newBuilder()
                        .setFriendsDrinksId(value.getFriendsDrinksId())
                        .setUserId(value.getUserId())
                        .build())
                        .leftJoin(pendingFriendsDrinksInvitations,
                                (request, state) -> {
                                    if (state != null) {
                                        return CreateFriendsDrinksInvitationReplyResponse
                                                .newBuilder()
                                                .setRequestId(request.getRequestId())
                                                .setResult(Result.SUCCESS)
                                                .build();
                                    } else {
                                        log.info("Rejecting invitation reply for {}", request.getRequestId());
                                        return CreateFriendsDrinksInvitationReplyResponse
                                                .newBuilder()
                                                .setRequestId(request.getRequestId())
                                                .setResult(Result.FAIL)
                                                .build();
                                    }
                                },
                                Joined.with(
                                        avro.friendsDrinksPendingInvitationIdSerde(),
                                        avro.createFriendsDrinksInvitationReplyRequestSerde(), avro.friendsDrinksPendingInvitationSerde())
                        );

        createFriendsDrinksInvitationReplyResponses.selectKey(((key, value) -> value.getRequestId()))
                .mapValues(value -> FriendsDrinksEvent
                        .newBuilder()
                        .setEventType(EventType.CREATE_FRIENDSDRINKS_INVITATION_REPLY_RESPONSE)
                        .setRequestId(value.getRequestId())
                        .setCreateFriendsDrinksInvitationReplyResponse(value)
                        .build())
                .to(apiTopicName, Produced.with(Serdes.String(), avro.apiFriendsDrinksSerde()));

        createFriendsDrinksInvitationReplyResponses.filter((key, value) -> value.getResult().equals(Result.SUCCESS))
                .mapValues(value -> (FriendsDrinksPendingInvitation) null)
                .to(pendingInvitationsTopicName,
                        Produced.with(avro.friendsDrinksPendingInvitationIdSerde(), avro.friendsDrinksPendingInvitationSerde()));
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("friendsdrinks-invitation-request.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        InvitationRequestService service = new InvitationRequestService();
        String registryUrl = envProps.getProperty("schema.registry.url");
        Topology topology = service.buildTopology(envProps, new FriendsDrinksAvro(registryUrl), new UserAvro(registryUrl));
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
