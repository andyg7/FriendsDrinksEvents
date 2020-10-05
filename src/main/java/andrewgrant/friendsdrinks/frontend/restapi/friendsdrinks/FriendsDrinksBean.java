package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for FriendsDrinksBean.
 */
public class FriendsDrinksBean {
    private FriendsDrinksIdBean friendsDrinksId;
    private List<String> userIds;
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getUserIds() {
        return userIds;
    }

    public void setUserIds(List<String> userIds) {
        this.userIds = userIds;
    }

    public FriendsDrinksIdBean getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(FriendsDrinksIdBean friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }
}
