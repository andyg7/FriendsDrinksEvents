package andrewgrant.friendsdrinks.membership;

import andrewgrant.friendsdrinks.api.avro.FriendsDrinksInvitationReplyRequest;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipEventType;
import andrewgrant.friendsdrinks.membership.avro.*;

/**
 * Emits events.
 */
public class MembershipRequestResponseJoiner {

    public FriendsDrinksMembershipEvent join(andrewgrant.friendsdrinks.api.avro.ApiEvent r) {
        if (r.getFriendsDrinksMembershipEvent().getEventType().equals(FriendsDrinksMembershipEventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST)) {
            FriendsDrinksInvitationReplyRequest request = r.getFriendsDrinksMembershipEvent()
                    .getFriendsDrinksInvitationReplyRequest();
            FriendsDrinksMembershipId membershipId = FriendsDrinksMembershipId
                    .newBuilder()
                    .setUserId(UserId.newBuilder().setUserId(request.getUserId().getUserId()).build())
                    .setFriendsDrinksId(FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(request.getFriendsDrinksId().getAdminUserId())
                            .setUuid(request.getFriendsDrinksId().getUuid())
                            .build())
                    .build();
            return FriendsDrinksMembershipEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.membership.avro.EventType.MEMBERSHIP_ADDED)
                    .setMembershipId(membershipId)
                    .setFriendsDrinksMembershipAdded(
                            FriendsDrinksMembershipAdded
                                    .newBuilder()
                                    .setMembershipId(membershipId)
                                    .build())
                    .build();
        } else if (r.getFriendsDrinksMembershipEvent().getEventType().equals(FriendsDrinksMembershipEventType.FRIENDSDRINKS_REMOVE_USER_REQUEST)) {
            FriendsDrinksInvitationReplyRequest request = r.getFriendsDrinksMembershipEvent().getFriendsDrinksInvitationReplyRequest();
            FriendsDrinksMembershipId membershipId = FriendsDrinksMembershipId
                    .newBuilder()
                    .setUserId(UserId.newBuilder().setUserId(request.getUserId().getUserId()).build())
                    .setFriendsDrinksId(FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(request.getFriendsDrinksId().getAdminUserId())
                            .setUuid(request.getFriendsDrinksId().getUuid())
                            .build())
                    .build();
            return FriendsDrinksMembershipEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.membership.avro.EventType.MEMBERSHIP_REMOVED)
                    .setMembershipId(membershipId)
                    .setFriendsDrinksMembershipRemoved(
                            FriendsDrinksMembershipRemoved
                                    .newBuilder()
                                    .setMembershipId(membershipId)
                                    .build())
                    .build();
        } else {
            throw new RuntimeException(
                    String.format("Received unexpected event type %s", r.getEventType().name()));
        }

    }
}
