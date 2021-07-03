package andrewgrant.friendsdrinks.frontend.api;

import andrewgrant.friendsdrinks.frontend.api.state.FriendsDrinksStateBean;

/**
 * Interface for retrieving state.
 */
public interface StateRetriever {
    FriendsDrinksStateBean getFriendsDrinksState(String uuid);
}
