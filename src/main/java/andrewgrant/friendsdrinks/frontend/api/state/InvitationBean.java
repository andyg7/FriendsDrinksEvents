package andrewgrant.friendsdrinks.frontend.api.state;

/**
 * Bean for invitation.
 */
public class InvitationBean {
    private String message;
    private String friendsDrinksId;
    private String friendsDrinksName;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }

    public String getFriendsDrinksName() {
        return friendsDrinksName;
    }

    public void setFriendsDrinksName(String friendsDrinksName) {
        this.friendsDrinksName = friendsDrinksName;
    }
}
