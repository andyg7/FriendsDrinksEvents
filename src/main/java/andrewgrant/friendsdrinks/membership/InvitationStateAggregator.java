package andrewgrant.friendsdrinks.membership;

import andrewgrant.friendsdrinks.avro.*;

/**
 * Aggregates invitation state.
 */
public class InvitationStateAggregator {

    public FriendsDrinksInvitationStateAggregate handleNewEvent(FriendsDrinksMembershipId aggKey,
                                                                FriendsDrinksInvitationEvent newValue,
                                                                FriendsDrinksInvitationStateAggregate aggValue) {
        FriendsDrinksInvitationState.Builder builder;
        if (aggValue.getFriendsDrinksInvitationState() == null) {
            builder = FriendsDrinksInvitationState.newBuilder();
            builder.setMembershipId(aggKey);
        } else {
            builder = FriendsDrinksInvitationState.newBuilder(aggValue.getFriendsDrinksInvitationState());
        }
        InvitationEventType eventType = newValue.getEventType();
        switch (eventType) {
            case CREATED:
                builder.setStatus(InvitationStatus.PENDING);
                builder.setMessage(newValue.getFriendsDrinksInvitationCreated().getMessage());
                break;
            case RESPONDED_TO:
                InvitationStatus invitationStatus;
                FriendsDrinksInvitationAnswer answer = newValue.getFriendsDrinksInvitationRespondedTo().getAnswer();
                switch (answer) {
                    case ACCEPTED:
                        invitationStatus = InvitationStatus.ACCEPTED;
                        break;
                    case REJECTED:
                        invitationStatus = InvitationStatus.REJECTED;
                        break;
                    default:
                        throw new RuntimeException(String.format("Unexpected event type %s", answer.name()));
                }
                builder.setStatus(invitationStatus);
                builder.setMessage(null);
                break;
            default:
                throw new RuntimeException(String.format("Unexpected event type %s", eventType.name()));
        }
        aggValue.setFriendsDrinksInvitationState(builder.build());
        return aggValue;
    }
}
