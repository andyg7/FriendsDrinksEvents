package andrewgrant.friendsdrinks.frontend.restapi.user;

import static andrewgrant.friendsdrinks.frontend.restapi.StreamsService.REQUESTS_STORE;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.user.avro.*;

/**
 * Implements frontend REST API users path.
 */
@Path("")
public class Handler {

    private KafkaProducer<UserId, UserEvent> userProducer;
    private Properties envProps;
    private KafkaStreams streams;

    public Handler(KafkaProducer<UserId, UserEvent> userProducer,
                   Properties envProps,
                   KafkaStreams streams) {
        this.userProducer = userProducer;
        this.envProps = envProps;
        this.streams = streams;
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public CreateUserResponseBean createUser(final CreateUserRequestBean createUserRequest)
            throws ExecutionException, InterruptedException {
        String userIdStr = UUID.randomUUID().toString();
        UserId userId = UserId.newBuilder()
                .setId(userIdStr)
                .build();
        String requestId = UUID.randomUUID().toString();
        CreateUserRequest request = CreateUserRequest.newBuilder()
                .setEmail(createUserRequest.getEmail())
                .setUserId(userId)
                .setRequestId(requestId)
                .build();
        UserEvent userEvent = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(request)
                .build();
        final String userTopicName = envProps.getProperty("user.topic.name");
        ProducerRecord<UserId, UserEvent> record =
                new ProducerRecord<>(
                        userTopicName,
                        userEvent.getCreateUserRequest().getUserId(),
                        userEvent);
        userProducer.send(record).get();

        ReadOnlyKeyValueStore<String, CreateUserResponse> kv =
                streams.store(REQUESTS_STORE,
                        QueryableStoreTypes.keyValueStore());

        CreateUserResponse createUserResponse = kv.get(requestId);
        if (createUserResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (createUserResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(500);
                createUserResponse = kv.get(requestId);
            }
        }
        if (createUserResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get CreateUserResponse for request id %s", requestId));
        }

        CreateUserResponseBean responseBean =
                new CreateUserResponseBean();

        Result result = createUserResponse.getResult();
        responseBean.setResult(result.toString());
        if (result.equals(Result.SUCCESS)) {
            // Only return user id if a user was actually created.
            responseBean.setUserId(createUserResponse.getUserId().getId());
        }

        return responseBean;
    }

    @DELETE
    @Path("/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public DeleteUserResponseBean deleteUser(@PathParam("userId") final String userIdStr)
            throws ExecutionException, InterruptedException {
        UserId userId = UserId.newBuilder()
                .setId(userIdStr)
                .build();
        String requestId = UUID.randomUUID().toString();
        DeleteUserRequest request = DeleteUserRequest.newBuilder()
                .setUserId(userId)
                .setRequestId(requestId)
                .build();
        UserEvent userEvent = UserEvent.newBuilder()
                .setEventType(EventType.DELETE_USER_REQUEST)
                .setDeleteUserRequest(request)
                .build();
        final String userTopicName = envProps.getProperty("user.topic.name");
        ProducerRecord<UserId, UserEvent> record =
                new ProducerRecord<>(
                        userTopicName,
                        userEvent.getDeleteUserRequest().getUserId(),
                        userEvent);
        userProducer.send(record).get();

        ReadOnlyKeyValueStore<String, DeleteUserResponse> kv =
                streams.store(REQUESTS_STORE,
                        QueryableStoreTypes.keyValueStore());

        DeleteUserResponse deleteUserResponse = kv.get(requestId);
        if (deleteUserResponse == null) {
            for (int i = 0; i < 10; i++) {
                if (deleteUserResponse != null) {
                    break;
                }
                // Give the backend some more time.
                Thread.sleep(500);
                deleteUserResponse = kv.get(requestId);
            }
        }
        if (deleteUserResponse == null) {
            throw new RuntimeException(String.format(
                    "Failed to get DeleteUserResponse for request id %s", requestId));
        }

        DeleteUserResponseBean responseBean =
                new DeleteUserResponseBean();

        responseBean.setResult(deleteUserResponse.getResult().toString());
        return responseBean;
    }

}
