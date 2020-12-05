package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.avro.*;

/**
 * Aggregates FriendsDrinksState.
 */
public class StateAggregator {

    public FriendsDrinksStateAggregate handleNewEvent(FriendsDrinksId aggKey, FriendsDrinksEvent newValue,
                                                      FriendsDrinksStateAggregate aggValue) {

            if (newValue.getEventType().equals(FriendsDrinksEventType.CREATED)) {
                FriendsDrinksCreated createdFriendsDrinks = newValue.getFriendsDrinksCreated();
                FriendsDrinksState.Builder friendsDrinksStateBuilder;
                if (aggValue.getFriendsDrinksState() == null) {
                    friendsDrinksStateBuilder = FriendsDrinksState.newBuilder();
                } else if (aggValue.getFriendsDrinksState().getStatus().equals(FriendsDrinksStatus.DELETED)) {
                    return aggValue;
                } else {
                    friendsDrinksStateBuilder = FriendsDrinksState
                            .newBuilder(aggValue.getFriendsDrinksState());
                }

                FriendsDrinksState friendsDrinksState = friendsDrinksStateBuilder
                        .setName(createdFriendsDrinks.getName())
                        .setFriendsDrinksId(FriendsDrinksId
                                .newBuilder()
                                .setUuid(newValue.getFriendsDrinksId().getUuid())
                                .build())
                        .setStatus(FriendsDrinksStatus.ACTIVE)
                        .setAdminUserId(createdFriendsDrinks.getAdminUserId())
                        .build();
                return FriendsDrinksStateAggregate.newBuilder()
                        .setFriendsDrinksState(friendsDrinksState)
                        .build();
            } else if (newValue.getEventType().equals(FriendsDrinksEventType.UPDATED)) {
                FriendsDrinksUpdated updatedFriendsDrinks = newValue.getFriendsDrinksUpdated();
                FriendsDrinksState.Builder friendsDrinksStateBuilder;
                if (aggValue.getFriendsDrinksState() == null) {
                    friendsDrinksStateBuilder = FriendsDrinksState.newBuilder();
                    friendsDrinksStateBuilder.setFriendsDrinksId(aggKey);
                    friendsDrinksStateBuilder.setStatus(FriendsDrinksStatus.ACTIVE);
                } else if (aggValue.getFriendsDrinksState().getStatus().equals(FriendsDrinksStatus.DELETED)) {
                    return aggValue;
                }
                UpdateType updateType = updatedFriendsDrinks.getUpdateType();
                friendsDrinksStateBuilder = FriendsDrinksState.newBuilder(aggValue.getFriendsDrinksState());
                String name;
                if (updatedFriendsDrinks.getName() != null) {
                    name = updatedFriendsDrinks.getName();
                } else if (updateType.equals(UpdateType.PARTIAL)) {
                    name = aggValue.getFriendsDrinksState().getName();
                } else if (updateType.equals(UpdateType.FULL)) {
                    name = null;
                } else {
                    throw new RuntimeException(String.format("Unknown update type %s", updateType.name()));
                }
                friendsDrinksStateBuilder.setName(name);

                return FriendsDrinksStateAggregate.newBuilder()
                        .setFriendsDrinksState(friendsDrinksStateBuilder.build())
                        .build();
            } else if (newValue.getEventType().equals(FriendsDrinksEventType.DELETED)) {
                FriendsDrinksState.Builder friendsDrinksStateBuilder;
                if (aggValue.getFriendsDrinksState() == null) {
                    friendsDrinksStateBuilder = FriendsDrinksState.newBuilder();
                    friendsDrinksStateBuilder.setFriendsDrinksId(aggKey);
                } else {
                    friendsDrinksStateBuilder = FriendsDrinksState.newBuilder(aggValue.getFriendsDrinksState());
                }
                friendsDrinksStateBuilder.setStatus(FriendsDrinksStatus.DELETED);
                return FriendsDrinksStateAggregate.newBuilder()
                        .setFriendsDrinksState(friendsDrinksStateBuilder.build())
                        .build();
            } else {
                throw new RuntimeException(String.format("Unexpected event type %s", newValue.getEventType().name()));
            }
    }
}
