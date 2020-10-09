package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetAllFriendsDrinksResponseBean.
 */
public class GetAllFriendsDrinksResponseBean {
    private List<String> friendsDrinkList;

    public List<String> getFriendsDrinkList() {
        return friendsDrinkList;
    }

    public void setFriendsDrinkList(List<String> friendsDrinkList) {
        this.friendsDrinkList = friendsDrinkList;
    }

}

