package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.User;

/**
 * Class to represent a request from a user for an email address.
 */
public class EmailRequest {
    private User userRequest;
    private Email currEmailState;

    public EmailRequest(User userRequest, Email currEmailState) {
        this.userRequest = userRequest;
        this.currEmailState = currEmailState;
    }

    public User getUserRequest() {
        return userRequest;
    }

    public Email getCurrEmailState() {
        return currEmailState;
    }
}
