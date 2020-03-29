package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.avro.*;


/**
 * Implements frontend REST API friendsdrinks path.
 */
@Path("")
public class Handler {

    private KafkaProducer<FriendsDrinksId, FriendsDrinksApi> kafkaProducer;
    private Properties envProps;

    public Handler(KafkaProducer<FriendsDrinksId, FriendsDrinksApi> kafkaProducer, Properties envProps) {
        this.kafkaProducer = kafkaProducer;
        this.envProps = envProps;
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public CreateFriendsDrinksResponseBean createFriendsDrinks(CreateFriendsDrinksRequestBean requestBean) {
        final String topicName = envProps.getProperty("friendsdrinks_api.topic.name");
        String requestId = UUID.randomUUID().toString();
        String friendsDrinksId = UUID.randomUUID().toString();
        CreateFriendsDrinksRequest createFriendsDrinksRequest = CreateFriendsDrinksRequest
                .newBuilder()
                .setFriendsDrinksId(FriendsDrinksId.newBuilder().setId(friendsDrinksId).build())
                .setUserIds(requestBean.getUserIds().stream().collect(Collectors.toList()))
                .setAdminUserId(requestBean.getAdminUserId())
                .setScheduleType(ScheduleType.valueOf(requestBean.getScheduleType()))
                .setCronSchedule(requestBean.getCronSchedule())
                .setRequestId(requestId)
                .build();
        FriendsDrinksApi friendsDrinksApi = FriendsDrinksApi
                .newBuilder()
                .setApiType(ApiType.CREATE_FRIENDS_DRINKS_REQUEST)
                .setCreateFriendsDrinksRequest(createFriendsDrinksRequest)
                .build();
        ProducerRecord<FriendsDrinksId, FriendsDrinksApi> record =
                new ProducerRecord<>(
                        topicName,
                        friendsDrinksApi.getCreateFriendsDrinksRequest().getFriendsDrinksId(),
                        friendsDrinksApi);
        kafkaProducer.send(record);

        CreateFriendsDrinksResponseBean responseBean = new CreateFriendsDrinksResponseBean();
        responseBean.setResult("SUCCESS");
        return responseBean;
    }
}
