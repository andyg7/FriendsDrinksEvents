package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.CreateUserRequest;
import andrewgrant.friendsdrinks.avro.Email;

/**
 * Class to represent a request from a user for an email address.
 */
public class CreateRequest {
    private CreateUserRequest createUserRequest;
    private Email currEmailState;

    public CreateRequest(CreateUserRequest createUserRequest, Email currEmailState) {
        this.createUserRequest = createUserRequest;
        this.currEmailState = currEmailState;
    }

    public CreateUserRequest getCreateUserRequest() {
        return createUserRequest;
    }

    public Email getCurrEmailState() {
        return currEmailState;
    }
}
