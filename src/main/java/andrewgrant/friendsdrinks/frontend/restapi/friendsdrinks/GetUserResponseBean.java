package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetUserResponse.
 */
public class GetUserResponseBean {

    private List<String> adminFriendsDrinksIds;
    private List<String> memberFriendsDrinksIds;
    private List<FriendsDrinksInvitationBean> invitations;

    public List<String> getAdminFriendsDrinksIds() {
        return adminFriendsDrinksIds;
    }

    public void setAdminFriendsDrinksIds(List<String> adminFriendsDrinksIds) {
        this.adminFriendsDrinksIds = adminFriendsDrinksIds;
    }

    public List<String> getMemberFriendsDrinksIds() {
        return memberFriendsDrinksIds;
    }

    public void setMemberFriendsDrinksIds(List<String> memberFriendsDrinksIds) {
        this.memberFriendsDrinksIds = memberFriendsDrinksIds;
    }

    public List<FriendsDrinksInvitationBean> getInvitations() {
        return invitations;
    }

    public void setInvitations(List<FriendsDrinksInvitationBean> invitations) {
        this.invitations = invitations;
    }
}
