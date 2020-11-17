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
                    .setUserId(UserId.newBuilder().setUserId(request.getMembershipId().getUserId().getUserId()).build())
                    .setFriendsDrinksId(FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(request.getMembershipId().getFriendsDrinksId().getAdminUserId())
                            .setUuid(request.getMembershipId().getFriendsDrinksId().getUuid())
                            .build())
                    .build();
            return FriendsDrinksMembershipEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.membership.avro.EventType.MEMBERSHIP_ADDED)
                    .setRequestId(r.getRequestId())
                    .setMembershipId(membershipId)
                    .setFriendsDrinksMembershipAdded(
                            FriendsDrinksMembershipAdded
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
