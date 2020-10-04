package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.api.avro.FriendsDrinksInvitationRequest;

/**
 * Holds aggregate result for FriendsDrinksInvitationRequest.
 */
public class InvitationTuple {
    public FriendsDrinksInvitationRequest invitationRequest;
    public boolean failed;
}
