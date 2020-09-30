package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post;

/**
 * DTO for POST request.
 */
public class PostUsersRequestBean {
    // Options for updateType: INVITE_FRIEND, REPLY_TO_INVITATION.
    private String updateType;
    // This is always required.
    private String friendsDrinksId;

    // Only relevant for INVITE_FRIEND.
    private String userId;
    // Only relevant for REPLY_TO_INVITATION.
    private String invitationReply;
    private String adminUserId;

    public PostUsersRequestBean() {
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }

    public String getInvitationReply() {
        return invitationReply;
    }

    public void setInvitationReply(String invitationReply) {
        this.invitationReply = invitationReply;
    }

    public String getUpdateType() {
        return updateType;
    }

    public void setUpdateType(String updateType) {
        this.updateType = updateType;
    }

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }


}
