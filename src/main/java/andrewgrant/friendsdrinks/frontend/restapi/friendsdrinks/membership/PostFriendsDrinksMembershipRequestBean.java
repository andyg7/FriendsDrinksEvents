package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.membership;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO for PostFriendsDrinksMembershipRequest.
 */
@JsonIgnoreProperties
public class PostFriendsDrinksMembershipRequestBean {

    public static final String ADD_USER = "ADD_USER";
    public static final String REMOVE_USER = "REMOVE_USER";
    public static final String REPLY_TO_INVITATION = "REPLY_TO_INVITATION";

    private String eventType;
    private AddUserRequestBean addUserRequest;
    private RemoveUserRequestBean removeUserRequest;
    private ReplyToInvitationRequestBean replyToInvitationRequest;

    public AddUserRequestBean getAddUserRequest() {
        return addUserRequest;
    }

    public void setAddUserRequest(AddUserRequestBean addUserRequest) {
        this.addUserRequest = addUserRequest;
    }

    public RemoveUserRequestBean getRemoveUserRequest() {
        return removeUserRequest;
    }

    public void setRemoveUserRequest(RemoveUserRequestBean removeUserRequest) {
        this.removeUserRequest = removeUserRequest;
    }

    public ReplyToInvitationRequestBean getReplyToInvitationRequest() {
        return replyToInvitationRequest;
    }

    public void setReplyToInvitationRequest(ReplyToInvitationRequestBean replyToInvitationRequest) {
        this.replyToInvitationRequest = replyToInvitationRequest;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
}
