package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for FriendsDrinksBean.
 */
public class FriendsDrinksBean {
    private String adminUserId;
    private String friendsDrinksId;
    private String name;

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }
}
