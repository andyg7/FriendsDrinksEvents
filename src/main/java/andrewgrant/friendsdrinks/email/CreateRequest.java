package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.user.avro.CreateUserRequest;

/**
 * Class to represent a request from a user for an email address.
 */
public class CreateRequest {
    private CreateUserRequest createUserRequest;
    private EmailEvent currEmailState;

    public CreateRequest(CreateUserRequest createUserRequest, EmailEvent currEmailState) {
        this.createUserRequest = createUserRequest;
        this.currEmailState = currEmailState;
    }

    public CreateUserRequest getCreateUserRequest() {
        return createUserRequest;
    }

    public EmailEvent getCurrEmailState() {
        return currEmailState;
    }
}