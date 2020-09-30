package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.api.avro.CreateFriendsDrinksInvitationRequest;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksPendingInvitation;

/**
 * Holds aggregate result for CreateFriendsDrinksInvitationRequest.
 */
public class CreateFriendsDrinksInvitationAggregateResult {
    public CreateFriendsDrinksInvitationRequest createFriendsDrinksInvitationRequest;
    public FriendsDrinksPendingInvitation friendsDrinksPendingInvitation;
    public boolean failed;
}
