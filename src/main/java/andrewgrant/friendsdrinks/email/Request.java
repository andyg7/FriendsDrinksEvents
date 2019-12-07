package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.UserRequest;

/**
 * Class to represent a request from a user for an email address.
 */
public class Request {
    private UserRequest userRequest;
    private Email currEmailState;

    public Request(UserRequest userRequest, Email currEmailState) {
        this.userRequest = userRequest;
        this.currEmailState = currEmailState;
    }

    public UserRequest getUserRequest() {
        return userRequest;
    }

    public Email getCurrEmailState() {
        return currEmailState;
    }
}
