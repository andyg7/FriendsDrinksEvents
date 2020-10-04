package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.api.avro.FriendsDrinksInvitationRequest;

/**
 * Holds result for FriendsDrinksInvitationRequest.
 */
public class InvitationResult {
    public FriendsDrinksInvitationRequest invitationRequest;
    public boolean failed;
}
