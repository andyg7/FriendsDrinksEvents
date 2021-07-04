package andrewgrant.friendsdrinks.frontend.api;

import java.util.List;

import andrewgrant.friendsdrinks.frontend.api.statestorebeans.*;

/**
 * Interface for retrieving state.
 */
public interface StateRetriever {
    FriendsDrinksStateBean getFriendsDrinksState(String uuid);

    List<FriendsDrinksStateBean> getAllFriendsDrinksStates();

    UserStateBean getUserState(String userId);

    List<UserStateBean> getAllUserStates();

    ApiResponseBean getApiResponse(String requestId);

    UserHomepageBean getUserHomePage(String userId);

    FriendsDrinksDetailPageBean getFriendsDrinksDetailPage(String friendsDrinksId);

    FriendsDrinksInvitationBean getInvitation(String friendsDrinksId, String userId);
}
