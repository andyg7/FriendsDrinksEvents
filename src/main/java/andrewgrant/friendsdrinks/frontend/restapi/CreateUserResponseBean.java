package andrewgrant.friendsdrinks.frontend.restapi;

/**
 * DTO for CreateUserResponse.
 */
public class CreateUserResponseBean {
    private String requestId;

    public CreateUserResponseBean() {
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
}