package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post;

/**
 * DTO for LoggedInEvent.
 */
public class LoggedInEventBean {
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
