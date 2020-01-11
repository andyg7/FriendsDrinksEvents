package andrewgrant.friendsdrinks.frontend.restapi.user;

/**
 * DTO for a CreateUserRequest.
 */
public class CreateUserRequestBean {
    private String email;

    public CreateUserRequestBean() {
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getEmail() {
        return email;
    }

}
