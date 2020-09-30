package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetAllFriendsDrinksResponseBean.
 */
public class GetAllFriendsDrinksResponseBean {
    private List<FriendsDrinksIdBean> friendsDrinkList;

    public List<FriendsDrinksIdBean> getFriendsDrinkList() {
        return friendsDrinkList;
    }

    public void setFriendsDrinkList(List<FriendsDrinksIdBean> friendsDrinkList) {
        this.friendsDrinkList = friendsDrinkList;
    }
}

