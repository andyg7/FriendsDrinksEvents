package andrewgrant.friendsdrinks.frontend.restapi.user;

/**
 * DTO for CreateUserResponse.
 */
public class CreateUserResponseBean {

    private String userId;
    private String result;

    public CreateUserResponseBean() {
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }


}
