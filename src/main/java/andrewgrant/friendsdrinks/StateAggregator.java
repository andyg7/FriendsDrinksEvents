package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.avro.*;

/**
 * Aggregates FriendsDrinksState.
 */
public class StateAggregator {

    public FriendsDrinksStateAggregate handleNewEvent(FriendsDrinksId aggKey, FriendsDrinksEvent newValue,
                                                      FriendsDrinksStateAggregate aggValue) {

            if (newValue.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.CREATED)) {
                FriendsDrinksCreated createdFriendsDrinks = newValue.getFriendsDrinksCreated();
                FriendsDrinksState.Builder friendsDrinksStateBuilder;
                if (aggValue.getFriendsDrinksState() == null) {
                    friendsDrinksStateBuilder = FriendsDrinksState.newBuilder();
                } else {
                    friendsDrinksStateBuilder = FriendsDrinksState
                            .newBuilder(aggValue.getFriendsDrinksState());
                }

                FriendsDrinksState friendsDrinksState = friendsDrinksStateBuilder
                        .setName(createdFriendsDrinks.getName())
                        .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                                .newBuilder()
                                .setUuid(newValue.getFriendsDrinksId().getUuid())
                                .setAdminUserId(newValue.getFriendsDrinksId().getAdminUserId())
                                .build())
                        .setStatus(Status.ACTIVE)
                        .build();
                return FriendsDrinksStateAggregate.newBuilder()
                        .setFriendsDrinksState(friendsDrinksState)
                        .build();
            } else if (newValue.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.UPDATED)) {
                FriendsDrinksUpdated updatedFriendsDrinks = newValue.getFriendsDrinksUpdated();
                FriendsDrinksState.Builder friendsDrinksStateBuilder;
                if (aggValue.getFriendsDrinksState() == null) {
                    friendsDrinksStateBuilder = FriendsDrinksState.newBuilder();
                    friendsDrinksStateBuilder.setFriendsDrinksId(aggKey);
                    friendsDrinksStateBuilder.setStatus(Status.ACTIVE);
                }
                andrewgrant.friendsdrinks.avro.UpdateType updateType = updatedFriendsDrinks.getUpdateType();
                friendsDrinksStateBuilder = FriendsDrinksState.newBuilder(aggValue.getFriendsDrinksState());
                String name;
                if (updatedFriendsDrinks.getName() != null) {
                    name = updatedFriendsDrinks.getName();
                } else if (updateType.equals(andrewgrant.friendsdrinks.avro.UpdateType.PARTIAL)) {
                    name = aggValue.getFriendsDrinksState().getName();
                } else if (updateType.equals(andrewgrant.friendsdrinks.avro.UpdateType.FULL)) {
                    name = null;
                } else {
                    throw new RuntimeException(String.format("Unknown update type %s", updateType.name()));
                }
                friendsDrinksStateBuilder.setName(name);

                return FriendsDrinksStateAggregate.newBuilder()
                        .setFriendsDrinksState(friendsDrinksStateBuilder.build())
                        .build();
            } else if (newValue.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.DELETED)) {
                FriendsDrinksState.Builder friendsDrinksStateBuilder;
                if (aggValue.getFriendsDrinksState() == null) {
                    friendsDrinksStateBuilder = FriendsDrinksState.newBuilder();
                    friendsDrinksStateBuilder.setFriendsDrinksId(aggKey);
                } else {
                    friendsDrinksStateBuilder = FriendsDrinksState.newBuilder(aggValue.getFriendsDrinksState());
                }
                friendsDrinksStateBuilder.setStatus(Status.DELETED);
                return FriendsDrinksStateAggregate.newBuilder()
                        .setFriendsDrinksState(friendsDrinksStateBuilder.build())
                        .build();
            } else {
                throw new RuntimeException(String.format("Unexpected event type %s", newValue.getEventType().name()));
            }
    }
}
