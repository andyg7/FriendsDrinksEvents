package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post;

/**
 * DTO for POST request.
 */
public class PostUsersRequestBean {
    // Options for updateType: INVITE_FRIEND, REPLY_TO_INVITATION.
    private String eventType;
    // This is always required.
    private String friendsDrinksUuid;

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

    public String getFriendsDrinksUuid() {
        return friendsDrinksUuid;
    }

    public void setFriendsDrinksUuid(String friendsDrinksUuid) {
        this.friendsDrinksUuid = friendsDrinksUuid;
    }

    public String getInvitationReply() {
        return invitationReply;
    }

    public void setInvitationReply(String invitationReply) {
        this.invitationReply = invitationReply;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }


}
