package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.UserEvent;

/**
 * Class to represent a request from a user for an email address.
 */
public class Request {
    private UserEvent userEvent;
    private Email currEmailState;

    public Request(UserEvent userEvent, Email currEmailState) {
        this.userEvent = userEvent;
        this.currEmailState = currEmailState;
    }

    public UserEvent getUserEvent() {
        return userEvent;
    }

    public Email getCurrEmailState() {
        return currEmailState;
    }
}
