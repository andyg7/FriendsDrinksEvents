package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for FriendsDrinksInvitation.
 */
public class FriendsDrinksInvitationBean {
    private String message;
    private FriendsDrinksIdBean friendsDrinksId;

    public FriendsDrinksIdBean getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(FriendsDrinksIdBean friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }


    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
