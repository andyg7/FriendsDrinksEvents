package andrewgrant.friendsdrinks.membership;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import andrewgrant.friendsdrinks.api.avro.EventType;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksInvitationReplyRequest;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksId;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipEvent;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksMembershipId;
import andrewgrant.friendsdrinks.membership.avro.FriendsDrinksUserAdded;

/**
 * Emits events.
 */
public class RequestResponseJoiner {

    private static final Logger log = LoggerFactory.getLogger(RequestResponseJoiner.class);

    public FriendsDrinksMembershipEvent join(andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent r) {

        if (r.getEventType().equals(EventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST)) {
            FriendsDrinksInvitationReplyRequest request = r.getFriendsDrinksInvitationReplyRequest();
            FriendsDrinksMembershipId membershipId = FriendsDrinksMembershipId
                            .newBuilder()
                            .setFriendsDrinksId(FriendsDrinksId
                                    .newBuilder()
                                    .setAdminUserId(request.getFriendsDrinksId().getAdminUserId())
                                    .setUuid(request.getFriendsDrinksId().getUuid())
                                    .build())
                            .build();
            return FriendsDrinksMembershipEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.membership.avro.EventType.USER_ADDED)
                    .setMembershipId(membershipId)
                    .setFriendsDrinksUserAdded(
                            FriendsDrinksUserAdded
                                    .newBuilder()
                                    .setMembershipId(membershipId)
                                    .build())
                    .build();
        } else {
            throw new RuntimeException(
                    String.format("Received unexpected event type %s", r.getEventType().toString()));
        }

    }
}
