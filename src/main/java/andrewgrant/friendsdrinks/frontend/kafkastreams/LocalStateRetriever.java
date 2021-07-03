package andrewgrant.friendsdrinks.frontend.kafkastreams;

import static andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService.FRIENDSDRINKS_STATE_STORE;
import static andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService.USERS_STATE_STORE;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import andrewgrant.friendsdrinks.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.avro.FriendsDrinksState;
import andrewgrant.friendsdrinks.avro.UserState;
import andrewgrant.friendsdrinks.frontend.api.StateRetriever;
import andrewgrant.friendsdrinks.frontend.api.state.FriendsDrinksStateBean;
import andrewgrant.friendsdrinks.frontend.api.state.UserStateBean;



/**
 * Gets state locally.
 */
public class LocalStateRetriever implements StateRetriever {

    private KafkaStreams kafkaStreams;

    public LocalStateRetriever(KafkaStreams kafkaStreams) {
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

    @Override
    public UserStateBean getUserState(String userId) {
        ReadOnlyKeyValueStore<String, UserState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USERS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        UserState userState = kv.get(userId);
        if (userState == null) {
            return null;
        }
        UserStateBean userStateBean = new UserStateBean();
        userStateBean.setUserId(userState.getUserId().getUserId());
        userStateBean.setFirstName(userState.getFirstName());
        userStateBean.setLastName(userState.getLastName());
        userStateBean.setEmail(userState.getEmail());
        return userStateBean;
    }
}
