package andrewgrant.friendsdrinks.frontend.api.statestorebeans;

/**
 * Bean for friends drinks state.
 */
public class FriendsDrinksStateBean {
    private String adminUserId;
    private String friendsDrinksId;
    private String name;
    private String status;

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
