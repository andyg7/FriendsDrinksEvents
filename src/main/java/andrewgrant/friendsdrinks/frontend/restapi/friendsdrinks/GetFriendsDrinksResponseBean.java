package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetFriendsDrinksResponse.
 */
public class GetFriendsDrinksResponseBean {
    private List<String> userIds;
    private String name;
    private FriendsDrinksIdBean friendsDrinksId;

    public FriendsDrinksIdBean getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(FriendsDrinksIdBean friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }


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
}
