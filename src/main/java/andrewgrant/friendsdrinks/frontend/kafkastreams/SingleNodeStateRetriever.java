package andrewgrant.friendsdrinks.frontend.kafkastreams;

import static andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService.FRIENDSDRINKS_STATE_STORE;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.frontend.api.StateRetriever;
import andrewgrant.friendsdrinks.frontend.api.state.FriendsDrinksStateBean;



/**
 * Gets state locally.
 */
public class SingleNodeStateRetriever implements StateRetriever {

    private KafkaStreams kafkaStreams;

    public SingleNodeStateRetriever(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public FriendsDrinksStateBean getFriendsDrinksState(String uuid) {
        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksState friendsDrinksState = kv.get(FriendsDrinksId.newBuilder().setUuid(uuid).build());
        if (friendsDrinksState == null) {
            return null;
        }
        FriendsDrinksStateBean friendsDrinksStateBean = new FriendsDrinksStateBean();
        friendsDrinksStateBean.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
        friendsDrinksStateBean.setStatus(friendsDrinksState.getStatus().name());
        friendsDrinksStateBean.setAdminUserId(friendsDrinksState.getAdminUserId());
        friendsDrinksStateBean.setName(friendsDrinksState.getName());
        return friendsDrinksStateBean;
    }
}
