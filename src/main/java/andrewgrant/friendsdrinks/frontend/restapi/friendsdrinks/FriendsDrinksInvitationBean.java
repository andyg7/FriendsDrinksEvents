package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for FriendsDrinksInvitation.
 */
public class FriendsDrinksInvitationBean {
    private String friendsDrinksId;
    private String adminUserId;
    private String message;

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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
