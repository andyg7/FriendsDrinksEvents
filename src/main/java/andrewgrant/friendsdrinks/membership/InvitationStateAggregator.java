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
                builder.setStatus(InvitationStatus.ACTIVE);
                builder.setMessage(newValue.getFriendsDrinksInvitationCreated().getMessage());
                builder.setResponse(null);
                break;
            case RESPONDED_TO:
                builder.setStatus(InvitationStatus.RESPONDED_TO);
                builder.setMessage(null);
                builder.setResponse(newValue.getFriendsDrinksInvitationRespondedTo().getResponse());
                break;
            default:
                throw new RuntimeException(String.format("Unexpected event type %s", eventType.name()));
        }
        aggValue.setFriendsDrinksInvitationState(builder.build());
        return aggValue;
    }
}
