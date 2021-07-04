package andrewgrant.friendsdrinks.frontend.api.statestorebeans;

/**
 * DTO for FriendsDrinksInvitation.
 */
public class FriendsDrinksInvitationBean {
    private String message;
    private String friendsDrinksId;
    private String friendsDrinksName;

    public String getFriendsDrinksName() {
        return friendsDrinksName;
    }

    public void setFriendsDrinksName(String friendsDrinksName) {
        this.friendsDrinksName = friendsDrinksName;
    }


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
