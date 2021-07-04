package andrewgrant.friendsdrinks.frontend.api.friendsdrinks;

import andrewgrant.friendsdrinks.frontend.api.statestorebeans.FriendsDrinksStateBean;

/**
 * DTO for GetFriendsDrinksResponse.
 */
public class GetFriendsDrinksResponseBean {
    private FriendsDrinksStateBean friendsDrinksStateBean;

    public FriendsDrinksStateBean getFriendsDrinksStateBean() {
        return friendsDrinksStateBean;
    }

    public void setFriendsDrinksStateBean(FriendsDrinksStateBean friendsDrinksStateBean) {
        this.friendsDrinksStateBean = friendsDrinksStateBean;
    }
}
