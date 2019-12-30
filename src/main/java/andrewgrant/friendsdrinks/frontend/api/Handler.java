package andrewgrant.friendsdrinks.frontend.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.user.avro.*;


/**
 * Implements frontend REST API for interacting with backend.
 */
@Path("v1")
public class Handler {

    private KafkaProducer<UserId, UserEvent> userProducer;
    private Properties envProps;
    private StreamsService streamsService;

    public Handler(KafkaProducer<UserId, UserEvent> userProducer,
                   Properties envProps,
                   StreamsService streamsService) {
        this.userProducer = userProducer;
        this.envProps = envProps;
        this.streamsService = streamsService;
    }

    @POST
    @Path("/user")
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
        CreateUserResponseBean responseBean =
                new CreateUserResponseBean();
        responseBean.setRequestId(requestId);
        return responseBean;
    }

    @GET
    @Path("/requests/{requestId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public GetUserResponseBean getUser(@PathParam("requestId") final String requestId) {
        ReadOnlyKeyValueStore<String, CreateUserResponse> kv = streamsService.getRequestsKvStore();
        CreateUserResponse createUserResponse = kv.get(requestId);
        String status;
        if (createUserResponse == null) {
            status = "Unknown";
        } else if (createUserResponse.getResult().equals(Result.SUCCESS)) {
            status = "Success";
        } else if (createUserResponse.getResult().equals(Result.FAIL)) {
            status = "Fail";
        } else {
            throw new RuntimeException("Failed to get status for request");
        }
        GetUserResponseBean responseBean = new GetUserResponseBean();
        responseBean.setStatus(status);
        return responseBean;
    }

    @GET
    @Path("/emails")
    @Produces(MediaType.APPLICATION_JSON)
    public GetEmailsResponseBean getEmails() {
        ReadOnlyKeyValueStore<String, String> kv = streamsService.getEmailsKvStore();
        KeyValueIterator<String, String> allKvs = kv.all();
        List<String> emails = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<String, String> keyValue = allKvs.next();
            emails.add(keyValue.key);
        }
        allKvs.close();
        GetEmailsResponseBean getEmailsResponseBean = new GetEmailsResponseBean();
        getEmailsResponseBean.setEmails(emails);
        return getEmailsResponseBean;
    }

}
