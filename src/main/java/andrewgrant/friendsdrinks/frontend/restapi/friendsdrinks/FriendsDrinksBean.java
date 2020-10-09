package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for FriendsDrinksBean.
 */
public class FriendsDrinksBean {
    private String friendsDrinksId;
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

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }
}
