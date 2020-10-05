package andrewgrant.friendsdrinks;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
                    friendsDrinksStateBuilder = FriendsDrinksState
                            .newBuilder();
                } else {
                    friendsDrinksStateBuilder = FriendsDrinksState
                            .newBuilder(aggValue.getFriendsDrinksState());
                }

                FriendsDrinksState friendsDrinksState =
                        friendsDrinksStateBuilder.setName(createdFriendsDrinks.getName())
                                .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                                        .newBuilder()
                                        .setUuid(
                                                newValue.getFriendsDrinksId().getUuid())
                                        .setAdminUserId(newValue.getFriendsDrinksId().getAdminUserId())
                                        .build())
                                .build();
                return FriendsDrinksStateAggregate.newBuilder()
                        .setFriendsDrinksState(friendsDrinksState)
                        .build();
            } else if (newValue.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.UPDATED)) {
                FriendsDrinksUpdated updatedFriendsDrinks = newValue.getFriendsDrinksUpdated();
                FriendsDrinksState.Builder friendsDrinksStateBuilder;
                if (aggValue.getFriendsDrinksState() == null) {
                    throw new RuntimeException(String.format("FriendsDrinksState is null for %s", aggKey));
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
                return null;
            } else if (newValue.getEventType().equals(andrewgrant.friendsdrinks.avro.EventType.USER_ADDED)) {
                FriendsDrinksState.Builder friendsDrinksStateBuilder =
                        FriendsDrinksState.newBuilder(aggValue.getFriendsDrinksState());
                List<String> currentUserIds = friendsDrinksStateBuilder.getUserIds();
                if (currentUserIds == null) {
                    currentUserIds = new ArrayList<>();
                }
                List<String> newUserIds = currentUserIds.stream().collect(Collectors.toList());
                newUserIds.add(newValue.getFriendsDrinksUserAdded().getUserId());
                friendsDrinksStateBuilder.setUserIds(newUserIds);
                return FriendsDrinksStateAggregate.newBuilder()
                        .setFriendsDrinksState(friendsDrinksStateBuilder.build())
                        .build();
            } else {
                throw new RuntimeException(String.format("Unexpected event type %s", newValue.getEventType().name()));
            }
    }
}
