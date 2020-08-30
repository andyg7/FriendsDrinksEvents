package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetAllFriendsDrinksResponseBean.
 */
public class GetAllFriendsDrinksResponseBean {
    private List<FriendsDrinksBean> friendsDrinkList;

    public List<FriendsDrinksBean> getFriendsDrinkList() {
        return friendsDrinkList;
    }

    public void setFriendsDrinkList(List<FriendsDrinksBean> friendsDrinkList) {
        this.friendsDrinkList = friendsDrinkList;
    }
}

