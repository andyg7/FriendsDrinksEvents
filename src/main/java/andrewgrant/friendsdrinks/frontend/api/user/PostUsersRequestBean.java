package andrewgrant.friendsdrinks.frontend.api.user;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO for POST request.
 */
@JsonIgnoreProperties
public class PostUsersRequestBean {

    public static final String LOGGED_IN = "LOGGED_IN";
    public static final String LOGGED_OUT = "LOGGED_OUT";
    public static final String SIGNED_OUT_SESSION_EXPIRED = "SIGNED_OUT_SESSION_EXPIRED";
    public static final String DELETED = "DELETED";

    private String eventType;

    private LoggedInEventBean loggedInEvent;

    public PostUsersRequestBean() {
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public LoggedInEventBean getLoggedInEvent() {
        return loggedInEvent;
    }

    public void setLoggedInEvent(LoggedInEventBean loggedInEvent) {
        this.loggedInEvent = loggedInEvent;
    }


}
