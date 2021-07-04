package andrewgrant.friendsdrinks.frontend.api.friendsdrinks;

import java.util.List;

import andrewgrant.friendsdrinks.frontend.api.statestorebeans.FriendsDrinksStateBean;

/**
 * DTO for GetAllFriendsDrinksResponseBean.
 */
public class GetAllFriendsDrinksResponseBean {
    private List<FriendsDrinksStateBean> friendsDrinkList;

    public List<FriendsDrinksStateBean> getFriendsDrinkList() {
        return friendsDrinkList;
    }

    public void setFriendsDrinkList(List<FriendsDrinksStateBean> friendsDrinkList) {
        this.friendsDrinkList = friendsDrinkList;
    }
}

