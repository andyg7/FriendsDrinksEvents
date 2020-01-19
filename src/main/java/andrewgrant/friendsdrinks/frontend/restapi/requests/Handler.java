package andrewgrant.friendsdrinks.frontend.restapi.requests;

import static andrewgrant.friendsdrinks.frontend.restapi.StreamsService.CREATE_USER_REQUESTS_STORE;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.user.avro.*;

/**
 * Implements frontend REST API for interacting with backend.
 */
@Path("")
public class Handler {

    private KafkaStreams streams;

    public Handler(KafkaStreams streams) {
        this.streams = streams;
    }

    @GET
    @Path("/{requestId}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetRequestResponseBean getRequest(@PathParam("requestId") final String requestId) {
        ReadOnlyKeyValueStore<String, CreateUserResponse> kv =
                streams.store(CREATE_USER_REQUESTS_STORE,
                        QueryableStoreTypes.keyValueStore());
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
        GetRequestResponseBean responseBean = new GetRequestResponseBean();
        responseBean.setStatus(status);
        return responseBean;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public GetRequestsResponseBean getRequests() {
        ReadOnlyKeyValueStore<String, CreateUserResponse> kv =
                streams.store(CREATE_USER_REQUESTS_STORE,
                        QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, CreateUserResponse> allKvs = kv.all();
        List<String> requests = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<String, CreateUserResponse> keyValue = allKvs.next();
            requests.add(keyValue.key);
        }
        allKvs.close();
        GetRequestsResponseBean response = new GetRequestsResponseBean();
        response.setRequests(requests);
        return response;
    }

}

