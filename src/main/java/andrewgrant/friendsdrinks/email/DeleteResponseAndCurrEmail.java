package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.DeleteUserResponse;
import andrewgrant.friendsdrinks.avro.Email;

/**
 * Simple DTO.
 */
public class DeleteResponseAndCurrEmail {
    private Email currEmailState;
    private DeleteUserResponse deleteUserResponse;

    public DeleteResponseAndCurrEmail(DeleteUserResponse deleteUserResponse, Email currEmailState) {
        this.deleteUserResponse = deleteUserResponse;
        this.currEmailState = currEmailState;
    }

    public DeleteUserResponse getDeleteUserResponse() {
        return deleteUserResponse;
    }

    public Email getCurrEmailState() {
        return currEmailState;
    }

}
