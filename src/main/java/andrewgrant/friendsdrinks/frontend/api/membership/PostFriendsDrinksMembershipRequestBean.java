package andrewgrant.friendsdrinks.frontend.api.membership;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO for PostFriendsDrinksMembershipRequest.
 */
@JsonIgnoreProperties
public class PostFriendsDrinksMembershipRequestBean {

    public static final String INVITE_USER = "INVITE_USER";
    public static final String REPLY_TO_INVITATION = "REPLY_TO_INVITATION";

    private String userId;
    private String friendsDrinksId;
    private String requestType;
    private InviteUserRequestBean inviteUserRequest;
    private ReplyToInvitationRequestBean replyToInvitationRequest;

    public InviteUserRequestBean getInviteUserRequest() {
        return inviteUserRequest;
    }

    public void setInviteUserRequest(InviteUserRequestBean addUserRequest) {
        this.inviteUserRequest = addUserRequest;
    }

    public ReplyToInvitationRequestBean getReplyToInvitationRequest() {
        return replyToInvitationRequest;
    }

    public void setReplyToInvitationRequest(ReplyToInvitationRequestBean replyToInvitationRequest) {
        this.replyToInvitationRequest = replyToInvitationRequest;
    }

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
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
}
