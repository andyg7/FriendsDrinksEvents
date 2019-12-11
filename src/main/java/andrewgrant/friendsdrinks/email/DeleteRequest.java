package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.DeleteUserRequest;
import andrewgrant.friendsdrinks.avro.Email;

/**
 * Class to represent a delete request from a user.
 */
public class DeleteRequest {
    private DeleteUserRequest deleteUserRequest;
    private Email currEmailState;

    public DeleteRequest(DeleteUserRequest deleteUserRequest, Email currEmailState) {
        this.deleteUserRequest = deleteUserRequest;
        this.currEmailState = currEmailState;
    }

    public DeleteUserRequest getDeleteUserRequest() {
        return deleteUserRequest;
    }

    public Email getCurrEmailState() {
        return currEmailState;
    }
}
