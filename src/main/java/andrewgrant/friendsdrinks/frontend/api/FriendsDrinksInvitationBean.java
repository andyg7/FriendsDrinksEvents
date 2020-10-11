package andrewgrant.friendsdrinks.frontend.api;

/**
 * DTO for FriendsDrinksInvitation.
 */
public class FriendsDrinksInvitationBean {
    private String message;
    private String friendsDrinksId;

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
