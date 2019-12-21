package andrewgrant.friendsdrinks.frontend.api;

/**
 * DTO for a CreateUserRequest.
 */
public class CreateUserRequestBean {

    public CreateUserRequestBean() {
    }

    public void setEmail(String email) {
        this.email = email;
    }

    private String email;

    public String getEmail() {
        return email;
    }

}
