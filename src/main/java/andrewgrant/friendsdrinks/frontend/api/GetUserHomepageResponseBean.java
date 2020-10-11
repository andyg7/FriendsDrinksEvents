package andrewgrant.friendsdrinks.frontend.api;

import java.util.List;

import andrewgrant.friendsdrinks.frontend.api.friendsdrinks.FriendsDrinksBean;
import andrewgrant.friendsdrinks.frontend.api.membership.FriendsDrinksInvitationBean;

/**
 * DTO for GetUserResponse.
 */
public class GetUserHomepageResponseBean {
    private List<FriendsDrinksBean> adminFriendsDrinksList;
    private List<FriendsDrinksBean> memberFriendsDrinksList;
    private List<FriendsDrinksInvitationBean> invitations;

    public List<FriendsDrinksBean> getAdminFriendsDrinksList() {
        return adminFriendsDrinksList;
    }

    public void setAdminFriendsDrinksList(List<FriendsDrinksBean> adminFriendsDrinksList) {
        this.adminFriendsDrinksList = adminFriendsDrinksList;
    }

    public List<FriendsDrinksBean> getMemberFriendsDrinksList() {
        return memberFriendsDrinksList;
    }

    public void setMemberFriendsDrinksList(List<FriendsDrinksBean> memberFriendsDrinksList) {
        this.memberFriendsDrinksList = memberFriendsDrinksList;
    }

    public List<FriendsDrinksInvitationBean> getInvitations() {
        return invitations;
    }

    public void setInvitations(List<FriendsDrinksInvitationBean> invitations) {
        this.invitations = invitations;
    }
}
