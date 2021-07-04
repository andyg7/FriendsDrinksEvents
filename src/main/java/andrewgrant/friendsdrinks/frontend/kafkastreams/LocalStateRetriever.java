package andrewgrant.friendsdrinks.frontend.kafkastreams;

import static andrewgrant.friendsdrinks.frontend.kafkastreams.MaterializedViewsService.*;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.frontend.api.StateRetriever;
import andrewgrant.friendsdrinks.frontend.api.statestorebeans.*;

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
    public List<FriendsDrinksStateBean> getAllFriendsDrinksStates() {
        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<FriendsDrinksId, FriendsDrinksState> allKvs = kv.all();
        List<FriendsDrinksStateBean> friendsDrinksStateBeanList = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<FriendsDrinksId, FriendsDrinksState> keyValue = allKvs.next();
            FriendsDrinksStateBean friendsDrinksStateBean = new FriendsDrinksStateBean();
            FriendsDrinksState friendsDrinksState = keyValue.value;
            friendsDrinksStateBean.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
            friendsDrinksStateBean.setAdminUserId(friendsDrinksState.getAdminUserId());
            friendsDrinksStateBean.setStatus(friendsDrinksState.getStatus().name());
            friendsDrinksStateBean.setName(friendsDrinksState.getName());
            friendsDrinksStateBeanList.add(friendsDrinksStateBean);
        }
        return friendsDrinksStateBeanList;
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
    public List<UserStateBean> getAllUserStates() {
        ReadOnlyKeyValueStore<String, UserState> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USERS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        KeyValueIterator<String, UserState> allKvs = kv.all();
        List<UserStateBean> userStateBeanList = new ArrayList<>();
        while (allKvs.hasNext()) {
            KeyValue<String, UserState> keyValue = allKvs.next();
            UserStateBean userStateBean = new UserStateBean();
            userStateBean.setUserId(keyValue.value.getUserId().getUserId());
            userStateBean.setFirstName(keyValue.value.getFirstName());
            userStateBean.setLastName(keyValue.value.getLastName());
            userStateBean.setEmail(keyValue.value.getEmail());
            userStateBeanList.add(userStateBean);
        }
        return userStateBeanList;
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

    @Override
    public UserHomepageBean getUserHomePage(String userId) {
        ReadOnlyKeyValueStore<String, UserHomepage> userHomepageStore =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USER_HOMEPAGES_STATE_STORE, QueryableStoreTypes.keyValueStore()));

        UserHomepage userHomepage = userHomepageStore.get(userId);
        if (userHomepage == null) {
            return null;
        }
        UserHomepageBean userHomepageBean = new UserHomepageBean();
        userHomepageBean.setUserId(userHomepage.getUserId());
        if (userHomepage.getAdminFriendsDrinks() != null && userHomepage.getAdminFriendsDrinks().getFriendsDrinks() != null)  {
            userHomepageBean.setAdminFriendsDrinksStateList(userHomepage.getAdminFriendsDrinks().getFriendsDrinks()
                    .stream().map(x -> {
                        FriendsDrinksStateBean friendsDrinksStateBean = new FriendsDrinksStateBean();
                        friendsDrinksStateBean.setStatus(x.getStatus().name());
                        friendsDrinksStateBean.setFriendsDrinksId(x.getFriendsDrinksId().getUuid());
                        friendsDrinksStateBean.setName(x.getName());
                        friendsDrinksStateBean.setAdminUserId(x.getAdminUserId());
                        return friendsDrinksStateBean;
                    }).collect(Collectors.toList()));
        }
        if (userHomepage.getMemberFriendsDrinks() != null && userHomepage.getMemberFriendsDrinks().getFriendsDrinks() != null)  {
            userHomepageBean.setMemberFriendsDrinksStateList(userHomepage.getMemberFriendsDrinks().getFriendsDrinks()
                    .stream().map(x -> {
                        FriendsDrinksStateBean friendsDrinksStateBean = new FriendsDrinksStateBean();
                        friendsDrinksStateBean.setStatus(x.getStatus().name());
                        friendsDrinksStateBean.setFriendsDrinksId(x.getFriendsDrinksId().getUuid());
                        friendsDrinksStateBean.setName(x.getName());
                        friendsDrinksStateBean.setAdminUserId(x.getAdminUserId());
                        return friendsDrinksStateBean;
                    }).collect(Collectors.toList()));
        }
        if (userHomepage.getInvitations() != null &&
                userHomepage.getInvitations().getInvitations() != null) {
            userHomepageBean.setInvitationBeanList(userHomepage.getInvitations().getInvitations()
                    .stream().map(x -> {
                        InvitationBean invitationBean = new InvitationBean();
                        invitationBean.setMessage(x.getMessage());
                        FriendsDrinksState friendsDrinksState = x.getFriendsDrinksState();
                        FriendsDrinksStateBean friendsDrinksStateBean = new FriendsDrinksStateBean();
                        friendsDrinksStateBean.setAdminUserId(friendsDrinksState.getAdminUserId());
                        friendsDrinksStateBean.setFriendsDrinksId(friendsDrinksState.getFriendsDrinksId().getUuid());
                        friendsDrinksStateBean.setStatus(friendsDrinksState.getStatus().name());
                        friendsDrinksStateBean.setName(friendsDrinksState.getName());
                        invitationBean.setFriendsDrinksStateBean(friendsDrinksStateBean);
                        return invitationBean;
                    }).collect(Collectors.toList()));
        }
        return userHomepageBean;
    }

    @Override
    public FriendsDrinksDetailPageBean getFriendsDrinksDetailPage(String friendsDrinksId) {
        ReadOnlyKeyValueStore<FriendsDrinksId, FriendsDrinksDetailPage> kv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(FRIENDSDRINKS_DETAIL_PAGE_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        FriendsDrinksDetailPage friendsDrinkDetailPage = kv.get(FriendsDrinksId.newBuilder().setUuid(friendsDrinksId).build());
        if (friendsDrinkDetailPage == null || friendsDrinkDetailPage.getStatus().equals(FriendsDrinksStatus.DELETED)) {
            return null;
        }
        FriendsDrinksDetailPageBean friendsDrinksDetailPageBean = new FriendsDrinksDetailPageBean();
        friendsDrinksDetailPageBean.setName(friendsDrinkDetailPage.getName());
        friendsDrinksDetailPageBean.setAdminUserId(friendsDrinkDetailPage.getAdminUserId());
        friendsDrinksDetailPageBean.setFriendsDrinksId(friendsDrinkDetailPage.getFriendsDrinksId().getUuid());
        if (friendsDrinkDetailPage.getMembers() != null) {
            friendsDrinksDetailPageBean.setMembers(friendsDrinkDetailPage.getMembers().stream()
                    .map(x -> {
                        UserStateBean userStateBean = new UserStateBean();
                        userStateBean.setUserId(x.getUserId().getUserId());
                        userStateBean.setEmail(x.getEmail());
                        userStateBean.setFirstName(x.getFirstName());
                        userStateBean.setLastName(x.getLastName());
                        return userStateBean;
                    }).collect(Collectors.toList()));
        }
        ReadOnlyKeyValueStore<String, UserState> usersKv =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType(USERS_STATE_STORE, QueryableStoreTypes.keyValueStore()));
        if (friendsDrinkDetailPage.getMeetups() != null) {
            friendsDrinksDetailPageBean.setFriendsDrinksDetailPageMeetupBeanList(friendsDrinkDetailPage.getMeetups()
                    .stream().map(x -> {
                        FriendsDrinksDetailPageMeetupBean meetupBean = new FriendsDrinksDetailPageMeetupBean();
                        meetupBean.setDate(x.getDate());
                        if (x.getUserIds() != null) {
                            meetupBean.setUserStateBeanList(x.getUserIds().stream().map(x1 -> {
                                UserStateBean userStateBean = new UserStateBean();
                                userStateBean.setUserId(x1.getUserId());
                                UserState userState = usersKv.get(x1.getUserId());
                                userStateBean.setEmail(userState.getEmail());
                                userStateBean.setFirstName(userState.getFirstName());
                                userStateBean.setLastName(userState.getLastName());
                                return userStateBean;
                            }).collect(Collectors.toList()));
                        }
                        return meetupBean;
                    }).collect(Collectors.toList()));
        }
        return friendsDrinksDetailPageBean;
    }
}
