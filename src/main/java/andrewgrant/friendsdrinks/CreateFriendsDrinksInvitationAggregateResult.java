package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.api.avro.CreateFriendsDrinksInvitationRequest;

/**
 * Holds aggregate result for CreateFriendsDrinksInvitationRequest.
 */
public class CreateFriendsDrinksInvitationAggregateResult {
    public CreateFriendsDrinksInvitationRequest createFriendsDrinksInvitationRequest;
    public String name;
    public boolean failed;
}
