package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.api.avro.FriendsDrinksRemoveUserRequest;

/**
 * Holds result for FriendsDrinksRemoveUserRequest.
 */
public class RemoveUserResult {
    public FriendsDrinksRemoveUserRequest friendsDrinksRemoveUserRequest;
    public boolean failed;
}
