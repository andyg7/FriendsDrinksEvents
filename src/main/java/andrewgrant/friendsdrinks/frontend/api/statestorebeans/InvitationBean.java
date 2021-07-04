package andrewgrant.friendsdrinks.frontend.api.statestorebeans;

/**
 * Bean for invitation.
 */
public class InvitationBean {
    private String message;
    private FriendsDrinksStateBean friendsDrinksStateBean;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public FriendsDrinksStateBean getFriendsDrinksStateBean() {
        return friendsDrinksStateBean;
    }

    public void setFriendsDrinksStateBean(FriendsDrinksStateBean friendsDrinksStateBean) {
        this.friendsDrinksStateBean = friendsDrinksStateBean;
    }
}
