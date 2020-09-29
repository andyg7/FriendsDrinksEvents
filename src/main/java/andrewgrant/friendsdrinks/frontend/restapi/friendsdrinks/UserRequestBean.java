package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for UserRequest.
 */
public class UserRequestBean {
    public UserRequestBean() {
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    private String eventType;
}
