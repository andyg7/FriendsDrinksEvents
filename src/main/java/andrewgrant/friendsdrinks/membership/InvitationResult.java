package andrewgrant.friendsdrinks.membership;

import andrewgrant.friendsdrinks.avro.FriendsDrinksInvitationRequest;

/**
 * Holds result for FriendsDrinksInvitationRequest.
 */
public class InvitationResult {
    public FriendsDrinksInvitationRequest invitationRequest;
    public boolean failed;
}
