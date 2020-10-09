package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO for POST request.
 */
@JsonIgnoreProperties
public class PostUsersRequestBean {
    public static final String SIGNED_UP = "SIGNED_UP";
    public static final String CANCELLED_ACCOUNT = "CANCELLED_ACCOUNT";

    // Options for updateType: INVITE_FRIEND, REPLY_TO_INVITATION.
    private String eventType;

    private String firstName;
    private String lastName;

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public PostUsersRequestBean() {
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

}
