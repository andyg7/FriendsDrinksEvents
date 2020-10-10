package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.user;

/**
 * DTO for SignedUpEvent.
 */
public class SignedUpEventBean {
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

}
