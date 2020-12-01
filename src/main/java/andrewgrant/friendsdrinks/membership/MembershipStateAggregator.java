package andrewgrant.friendsdrinks.membership;

import andrewgrant.friendsdrinks.membership.avro.*;

/**
 * Aggregates membership state.
 */
public class MembershipStateAggregator {

    FriendsDrinksMembershipStateAggregate handleNewEvent(FriendsDrinksMembershipId aggKey,
                                                         FriendsDrinksMembershipEvent newValue,
                                                         FriendsDrinksMembershipStateAggregate aggValue) {
        FriendsDrinksMembershipState.Builder builder;
        if (aggValue.getFriendsDrinksMembershipState() == null) {
            builder = FriendsDrinksMembershipState.newBuilder();
            builder.setMembershipId(aggKey);
        } else {
            builder = FriendsDrinksMembershipState.newBuilder(aggValue.getFriendsDrinksMembershipState());
        }
        EventType eventType = newValue.getEventType();
        switch (eventType) {
            case ADDED:
                builder.setStatus(Status.ACTIVE);
                break;
            case REMOVED:
                builder.setStatus(Status.REMOVED);
                break;
            default:
                throw new RuntimeException(String.format("Unexpected event type %s", eventType.name()));
        }
        aggValue.setFriendsDrinksMembershipState(builder.build());
        return aggValue;
    }
}
