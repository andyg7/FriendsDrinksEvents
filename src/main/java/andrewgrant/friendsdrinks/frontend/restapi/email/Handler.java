package andrewgrant.friendsdrinks.frontend.restapi.email;

import static andrewgrant.friendsdrinks.frontend.restapi.StreamsService.EMAILS_STORE;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

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
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public GetEmailsResponseBean getEmails() {
        ReadOnlyKeyValueStore<String, String> kv =
                streams.store(EMAILS_STORE,
                        QueryableStoreTypes.keyValueStore());
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

    @GET
    @Path("/{email}")
    @Produces(MediaType.APPLICATION_JSON)
    public GetEmailResponseBean getEmail(@PathParam("email") final String email) {
        ReadOnlyKeyValueStore<String, String> kv =
                streams.store(EMAILS_STORE,
                        QueryableStoreTypes.keyValueStore());
        String userId = kv.get(email);
        GetEmailResponseBean getEmailResponseBean = new GetEmailResponseBean();
        getEmailResponseBean.setUserId(userId);
        return getEmailResponseBean;
    }

}

