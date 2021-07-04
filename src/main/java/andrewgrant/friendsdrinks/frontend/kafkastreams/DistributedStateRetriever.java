package andrewgrant.friendsdrinks.frontend.kafkastreams;

import static andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService.*;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.AvroBuilder;
import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.frontend.api.StateRetriever;
import andrewgrant.friendsdrinks.frontend.api.state.*;

/**
 * Retrieves state from across Kafka Streams application.
 */
public class DistributedStateRetriever implements StateRetriever {

    private static final Logger log = LoggerFactory.getLogger(DistributedStateRetriever.class);

    private KafkaStreams kafkaStreams;
    private andrewgrant.friendsdrinks.AvroBuilder avroBuilder;
    private andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder;
    private andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder;
    private andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder;
    private andrewgrant.friendsdrinks.meetup.AvroBuilder meetupAvroBuilder;
    private Client client;

    public DistributedStateRetriever(KafkaStreams kafkaStreams, AvroBuilder avroBuilder,
                                     andrewgrant.friendsdrinks.frontend.AvroBuilder apiAvroBuilder,
                                     andrewgrant.friendsdrinks.membership.AvroBuilder membershipAvroBuilder,
                                     andrewgrant.friendsdrinks.user.AvroBuilder userAvroBuilder,
                                     andrewgrant.friendsdrinks.meetup.AvroBuilder meetupAvroBuilder) {
        this.kafkaStreams = kafkaStreams;
        this.avroBuilder = avroBuilder;
        this.apiAvroBuilder = apiAvroBuilder;
        this.membershipAvroBuilder = membershipAvroBuilder;
        this.userAvroBuilder = userAvroBuilder;
        this.meetupAvroBuilder = meetupAvroBuilder;
        client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    }

    @Override
    public FriendsDrinksStateBean getFriendsDrinksState(String uuid) {
        FriendsDrinksId key = FriendsDrinksId.newBuilder().setUuid(uuid).build();
        KeyQueryMetadata keyQueryMetadata = kafkaStreams.queryMetadataForKey(FRIENDSDRINKS_STATE_STORE, key,
                avroBuilder.friendsDrinksIdSerde().serializer());
        if (keyQueryMetadata == null) {
            return null;
        }
        HostInfo hostInfo = keyQueryMetadata.activeHost();
        log.info("Host info: {} {}", hostInfo.host(), hostInfo.port());
        return client.target(endpointWithKey(hostInfo, FRIENDSDRINKS_STATE_STORE, uuid))
                .request(MediaType.APPLICATION_JSON)
                .get(new GenericType<FriendsDrinksStateBean>(){});
    }

    @Override
    public List<FriendsDrinksStateBean> getAllFriendsDrinksStates() {
        Collection<StreamsMetadata> streamsMetadataCollection = kafkaStreams.allMetadataForStore(FRIENDSDRINKS_STATE_STORE);
        List<FriendsDrinksStateBean> friendsDrinksStateBeanList = new ArrayList<>();
        for (StreamsMetadata streamsMetadata : streamsMetadataCollection) {
            FriendsDrinksStateBean friendsDrinksStateBean = client.target(endpoint(streamsMetadata.hostInfo(), FRIENDSDRINKS_STATE_STORE))
                    .request(MediaType.APPLICATION_JSON)
                    .get(new GenericType<FriendsDrinksStateBean>(){});
            friendsDrinksStateBeanList.add(friendsDrinksStateBean);
        }
        return friendsDrinksStateBeanList;
    }

    @Override
    public UserStateBean getUserState(String userId) {
        KeyQueryMetadata keyQueryMetadata = kafkaStreams.queryMetadataForKey(USERS_STATE_STORE, userId,
                Serdes.String().serializer());
        if (keyQueryMetadata == null) {
            return null;
        }
        HostInfo hostInfo = keyQueryMetadata.activeHost();
        log.info("Host info: {} {}", hostInfo.host(), hostInfo.port());
        return client.target(endpointWithKey(hostInfo, USERS_STATE_STORE, userId))
                .request(MediaType.APPLICATION_JSON)
                .get(new GenericType<UserStateBean>(){});
    }

    @Override
    public List<UserStateBean> getAllUserStates() {
        Collection<StreamsMetadata> streamsMetadataCollection = kafkaStreams.allMetadataForStore(USERS_STATE_STORE);
        List<UserStateBean> userStateBeanList = new ArrayList<>();
        for (StreamsMetadata streamsMetadata : streamsMetadataCollection) {
            UserStateBean userStateBean = client.target(endpoint(streamsMetadata.hostInfo(), USERS_STATE_STORE))
                    .request(MediaType.APPLICATION_JSON)
                    .get(new GenericType<UserStateBean>(){});
            userStateBeanList.add(userStateBean);
        }
        return userStateBeanList;
    }

    @Override
    public ApiResponseBean getApiResponse(String requestId) {
        KeyQueryMetadata keyQueryMetadata = kafkaStreams.queryMetadataForKey(RESPONSES_STATE_STORE, requestId,
                Serdes.String().serializer());
        if (keyQueryMetadata == null) {
            return null;
        }
        HostInfo hostInfo = keyQueryMetadata.activeHost();
        log.info("Host info: {} {}", hostInfo.host(), hostInfo.port());
        return client.target(endpointWithKey(hostInfo, RESPONSES_STATE_STORE, requestId))
                .request(MediaType.APPLICATION_JSON)
                .get(new GenericType<ApiResponseBean>(){});
    }

    @Override
    public UserHomepageBean getUserHomePage(String userId) {
        KeyQueryMetadata keyQueryMetadata = kafkaStreams.queryMetadataForKey(USER_HOMEPAGES_STATE_STORE, userId,
                Serdes.String().serializer());
        if (keyQueryMetadata == null) {
            return null;
        }
        HostInfo hostInfo = keyQueryMetadata.activeHost();
        log.info("Host info: {} {}", hostInfo.host(), hostInfo.port());
        return client.target(endpointWithKey(hostInfo, USER_HOMEPAGES_STATE_STORE, userId))
                .request(MediaType.APPLICATION_JSON)
                .get(new GenericType<UserHomepageBean>(){});
    }

    @Override
    public FriendsDrinksDetailPageBean getFriendsDrinksDetailPage(String uuid) {
        FriendsDrinksId key = FriendsDrinksId.newBuilder().setUuid(uuid).build();
        KeyQueryMetadata keyQueryMetadata = kafkaStreams.queryMetadataForKey(FRIENDSDRINKS_DETAIL_PAGE_STATE_STORE, key,
                avroBuilder.friendsDrinksIdSerde().serializer());
        if (keyQueryMetadata == null) {
            return null;
        }
        HostInfo hostInfo = keyQueryMetadata.activeHost();
        log.info("Host info: {} {}", hostInfo.host(), hostInfo.port());
        return client.target(endpointWithKey(hostInfo, FRIENDSDRINKS_DETAIL_PAGE_STATE_STORE, uuid))
                .request(MediaType.APPLICATION_JSON)
                .get(new GenericType<FriendsDrinksDetailPageBean>(){});
    }

    private String endpointWithKey(HostInfo hostInfo, String stateStoreName, String key) {
        return String.format("http://%s:%d/%s/%s", hostInfo.host(), hostInfo.port(), stateStoreName, key);
    }

    private String endpoint(HostInfo hostInfo, String stateStoreName) {
        return String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), stateStoreName);
    }
}
