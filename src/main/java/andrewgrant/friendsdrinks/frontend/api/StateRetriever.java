package andrewgrant.friendsdrinks.frontend.api;

import andrewgrant.friendsdrinks.frontend.api.state.ApiResponseBean;
import andrewgrant.friendsdrinks.frontend.api.state.FriendsDrinksStateBean;
import andrewgrant.friendsdrinks.frontend.api.state.UserHomepageBean;
import andrewgrant.friendsdrinks.frontend.api.state.UserStateBean;

/**
 * Interface for retrieving state.
 */
public interface StateRetriever {
    FriendsDrinksStateBean getFriendsDrinksState(String uuid);

    UserStateBean getUserState(String userId);

    ApiResponseBean getApiResponse(String requestId);

    UserHomepageBean getUserHomePage(String userId);
}
