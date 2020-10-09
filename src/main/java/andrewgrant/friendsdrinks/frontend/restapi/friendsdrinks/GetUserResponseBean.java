package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetUserResponse.
 */
public class GetUserResponseBean {
    private List<FriendsDrinksIdBean> adminFriendsDrinksIds;
    private List<FriendsDrinksIdBean> memberFriendsDrinksIds;
    private List<FriendsDrinksInvitationBean> invitations;

    public List<FriendsDrinksIdBean> getMemberFriendsDrinksIds() {
        return memberFriendsDrinksIds;
    }

    public void setMemberFriendsDrinksIds(List<FriendsDrinksIdBean> memberFriendsDrinksIds) {
        this.memberFriendsDrinksIds = memberFriendsDrinksIds;
    }

    public List<FriendsDrinksIdBean> getAdminFriendsDrinksIds() {
        return adminFriendsDrinksIds;
    }

    public void setAdminFriendsDrinksIds(List<FriendsDrinksIdBean> adminFriendsDrinksIds) {
        this.adminFriendsDrinksIds = adminFriendsDrinksIds;
    }

    public List<FriendsDrinksInvitationBean> getInvitations() {
        return invitations;
    }

    public void setInvitations(List<FriendsDrinksInvitationBean> invitations) {
        this.invitations = invitations;
    }
}
