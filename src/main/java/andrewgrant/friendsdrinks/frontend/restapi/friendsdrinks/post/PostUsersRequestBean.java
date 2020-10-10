package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO for POST request.
 */
@JsonIgnoreProperties
public class PostUsersRequestBean {

    public static final String SIGNED_UP = "SIGNED_UP";
    public static final String CANCELLED_ACCOUNT = "CANCELLED_ACCOUNT";
    public static final String LOGGED_IN = "LOGGED_IN";
    public static final String LOGGED_OUT = "LOGGED_OUT";
    public static final String SIGNED_OUT_SESSION_EXPIRED = "SIGNED_OUT_SESSION_EXPIRED";

    private String eventType;

    private SignedUpEventBean signedUpEvent;
    private LoggedInEventBean loggedInEvent;

    public PostUsersRequestBean() {
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public SignedUpEventBean getSignedUpEvent() {
        return signedUpEvent;
    }

    public void setSignedUpEvent(SignedUpEventBean signedUpEvent) {
        this.signedUpEvent = signedUpEvent;
    }

    public LoggedInEventBean getLoggedInEvent() {
        return loggedInEvent;
    }

    public void setLoggedInEvent(LoggedInEventBean loggedInEvent) {
        this.loggedInEvent = loggedInEvent;
    }


}
