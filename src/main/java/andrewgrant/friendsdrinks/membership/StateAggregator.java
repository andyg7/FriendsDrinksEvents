package andrewgrant.friendsdrinks.membership;

import andrewgrant.friendsdrinks.membership.avro.*;

/**
 * Aggregates membership state.
 */
public class StateAggregator {

    FriendsDrinksMembershipStateAggregate handleNewEvent(FriendsDrinksMembershipId aggKey,
                                                         FriendsDrinksMembershipEvent newValue,
                                                         FriendsDrinksMembershipStateAggregate aggValue) {
        if (aggValue.getFriendsDrinksMembershipState() != null &&
                aggValue.getFriendsDrinksMembershipState().getStatus().equals(Status.DELETED)) {
            return aggValue;
        }
        FriendsDrinksMembershipState.Builder builder;
        if (aggValue.getFriendsDrinksMembershipState() == null) {
            builder = FriendsDrinksMembershipState.newBuilder();
        } else {
            builder = FriendsDrinksMembershipState.newBuilder(aggValue.getFriendsDrinksMembershipState());
        }
        EventType eventType = newValue.getEventType();
        switch (eventType) {
            case ADDED:
                builder.setMembershipId(newValue.getMembershipId());
                builder.setStatus(Status.ACTIVE);
                break;
            case REMOVED:
                builder.setMembershipId(newValue.getMembershipId());
                builder.setStatus(Status.REMOVED);
                break;
            case DELETED:
                builder.setMembershipId(newValue.getMembershipId());
                builder.setStatus(Status.DELETED);
                break;
            default:
                throw new RuntimeException(String.format("Unexpected event type %s", eventType.name()));
        }
        aggValue.setFriendsDrinksMembershipState(builder.build());
        return aggValue;
    }
}