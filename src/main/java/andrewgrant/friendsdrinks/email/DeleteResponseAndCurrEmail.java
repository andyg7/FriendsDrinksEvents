package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.DeleteUserResponse;
import andrewgrant.friendsdrinks.email.avro.EmailEvent;

/**
 * Simple DTO.
 */
public class DeleteResponseAndCurrEmail {
    private EmailEvent currEmailState;
    private DeleteUserResponse deleteUserResponse;

    public DeleteResponseAndCurrEmail(
            DeleteUserResponse deleteUserResponse,
            EmailEvent currEmailState) {
        this.deleteUserResponse = deleteUserResponse;
        this.currEmailState = currEmailState;
    }

    public DeleteUserResponse getDeleteUserResponse() {
        return deleteUserResponse;
    }

    public EmailEvent getCurrEmailState() {
        return currEmailState;
    }

}
