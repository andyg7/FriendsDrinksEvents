package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.user.avro.DeleteUserRequest;

/**
 * Class to represent a delete request from a user.
 */
public class DeleteRequestAndCurrEmail {
    private DeleteUserRequest deleteUserRequest;
    private EmailEvent currEmailState;

    public DeleteRequestAndCurrEmail(
            DeleteUserRequest deleteUserRequest,
            EmailEvent currEmailState) {
        this.deleteUserRequest = deleteUserRequest;
        this.currEmailState = currEmailState;
    }

    public DeleteUserRequest getDeleteUserRequest() {
        return deleteUserRequest;
    }

    public EmailEvent getCurrEmailState() {
        return currEmailState;
    }
}
