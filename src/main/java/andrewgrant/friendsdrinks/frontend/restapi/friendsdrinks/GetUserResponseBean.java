package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetUserResponse.
 */
public class GetUserResponseBean {
    private List<FriendsDrinksBean> adminFriendsDrinks;
    private List<FriendsDrinksBean> memberFriendsDrinks;
    private List<FriendsDrinksInvitationBean> invitations;

    public List<FriendsDrinksBean> getAdminFriendsDrinks() {
        return adminFriendsDrinks;
    }

    public void setAdminFriendsDrinks(List<FriendsDrinksBean> adminFriendsDrinks) {
        this.adminFriendsDrinks = adminFriendsDrinks;
    }

    public List<FriendsDrinksBean> getMemberFriendsDrinks() {
        return memberFriendsDrinks;
    }

    public void setMemberFriendsDrinks(List<FriendsDrinksBean> memberFriendsDrinks) {
        this.memberFriendsDrinks = memberFriendsDrinks;
    }

    public List<FriendsDrinksInvitationBean> getInvitations() {
        return invitations;
    }

    public void setInvitations(List<FriendsDrinksInvitationBean> invitations) {
        this.invitations = invitations;
    }
}
