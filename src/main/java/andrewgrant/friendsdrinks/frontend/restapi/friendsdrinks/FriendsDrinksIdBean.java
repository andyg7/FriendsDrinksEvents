package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for FriendsDrinksId.
 */
public class FriendsDrinksIdBean {
    private String adminUserId;
    private String friendsDrinksId;

    public FriendsDrinksIdBean(String adminUserId, String friendsDrinksId) {
        this.adminUserId = adminUserId;
        this.friendsDrinksId = friendsDrinksId;
    }

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }
}
