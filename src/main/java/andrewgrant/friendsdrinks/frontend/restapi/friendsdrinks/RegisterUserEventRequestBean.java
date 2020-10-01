package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for UserRequest.
 */
public class RegisterUserEventRequestBean {
    private String eventType;
    private String userId;

    public RegisterUserEventRequestBean() {
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

}
