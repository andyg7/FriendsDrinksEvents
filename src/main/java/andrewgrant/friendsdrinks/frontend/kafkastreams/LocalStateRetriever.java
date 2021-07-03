package andrewgrant.friendsdrinks.frontend.kafkastreams;

import static andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService.*;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.frontend.api.StateRetriever;
import andrewgrant.friendsdrinks.frontend.api.state.ApiResponseBean;
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

    @Override
    public ApiResponseBean getApiResponse(String requestId) {
        ReadOnlyKeyValueStore<String, ApiEvent> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(RESPONSES_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        ApiResponseBean apiResponseBean = new ApiResponseBean();
        ApiEvent backendResponse = kv.get(requestId);
        if (backendResponse.getEventType().equals(ApiEventType.FRIENDSDRINKS_MEMBERSHIP_EVENT)) {
            if (backendResponse.getFriendsDrinksMembershipEvent().getEventType()
                    .equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_REPLY_RESPONSE)) {
                apiResponseBean.setResult(backendResponse.getFriendsDrinksMembershipEvent()
                        .getFriendsDrinksInvitationReplyResponse().getResult().name());
            } else if (backendResponse.getFriendsDrinksMembershipEvent().getEventType()
                    .equals(FriendsDrinksMembershipApiEventType.FRIENDSDRINKS_INVITATION_RESPONSE)) {
                apiResponseBean.setResult(backendResponse.getFriendsDrinksMembershipEvent().getFriendsDrinksInvitationResponse().getResult().name());
            } else {
                throw new RuntimeException(String.format("Unknown membership event type %s",
                        backendResponse.getFriendsDrinksMembershipEvent().getEventType().name()));
            }
        } else if (backendResponse.getEventType().equals(ApiEventType.FRIENDSDRINKS_EVENT)) {
            FriendsDrinksApiEvent friendsDrinksApiEvent = backendResponse.getFriendsDrinksEvent();
            FriendsDrinksApiEventType eventType = friendsDrinksApiEvent.getEventType();
            if (eventType.equals(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_RESPONSE)) {
                apiResponseBean.setResult(friendsDrinksApiEvent.getCreateFriendsDrinksResponse().getResult().name());
            } else if (eventType.equals(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_RESPONSE)) {
                apiResponseBean.setResult(friendsDrinksApiEvent.getUpdateFriendsDrinksResponse().getResult().name());
            } else if (eventType.equals(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_RESPONSE)) {
                apiResponseBean.setResult(friendsDrinksApiEvent.getDeleteFriendsDrinksResponse().getResult().name());
            } else {
                throw new RuntimeException(String.format("Unknown event type %s", eventType.name()));
            }
        } else {
            throw new RuntimeException(String.format("Unknown api event type %s", backendResponse.getEventType().name()));
        }
        return apiResponseBean;
    }
}
