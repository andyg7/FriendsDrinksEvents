package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for FriendsDrinksId.
 */
public class FriendsDrinksIdBean {
    private String adminUserId;
    private String uuid;

    public FriendsDrinksIdBean(String adminUserId, String uuid) {
        this.adminUserId = adminUserId;
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }
}
