package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetUserResponse.
 */
public class GetUserResponseBean {
    private List<FriendsDrinksIdBean> adminFriendsDrinksIdBeans;
    private List<FriendsDrinksIdBean> memberFriendsDrinksIdBeans;
    private List<FriendsDrinksInvitationBean> invitations;

    public List<FriendsDrinksIdBean> getAdminFriendsDrinksIdBeans() {
        return adminFriendsDrinksIdBeans;
    }

    public void setAdminFriendsDrinksIdBeans(List<FriendsDrinksIdBean> adminFriendsDrinksIdBeans) {
        this.adminFriendsDrinksIdBeans = adminFriendsDrinksIdBeans;
    }

    public List<FriendsDrinksIdBean> getMemberFriendsDrinksIdBeans() {
        return memberFriendsDrinksIdBeans;
    }

    public void setMemberFriendsDrinksIdBeans(List<FriendsDrinksIdBean> memberFriendsDrinksIdBeans) {
        this.memberFriendsDrinksIdBeans = memberFriendsDrinksIdBeans;
    }

    public List<FriendsDrinksInvitationBean> getInvitations() {
        return invitations;
    }

    public void setInvitations(List<FriendsDrinksInvitationBean> invitations) {
        this.invitations = invitations;
    }
}
