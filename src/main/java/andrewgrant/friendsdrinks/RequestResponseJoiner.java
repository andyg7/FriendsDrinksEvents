package andrewgrant.friendsdrinks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import andrewgrant.friendsdrinks.avro.*;

/**
 * Emits FriendsDrinks events from API results.
 */
public class RequestResponseJoiner {

    private static final Logger log = LoggerFactory.getLogger(RequestResponseJoiner.class);

    public FriendsDrinksEvent join(andrewgrant.friendsdrinks.avro.FriendsDrinksApiEvent r) {
        if (r.getEventType().equals(FriendsDrinksApiEventType.CREATE_FRIENDSDRINKS_REQUEST)) {
            log.info("Got create join {}", r.getCreateFriendsDrinksRequest().getRequestId());
            CreateFriendsDrinksRequest createFriendsDrinksRequest =
                    r.getCreateFriendsDrinksRequest();
            FriendsDrinksCreated friendsDrinks = FriendsDrinksCreated
                    .newBuilder()
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(createFriendsDrinksRequest.getFriendsDrinksId().getUuid())
                            .build())
                    .setName(createFriendsDrinksRequest.getName())
                    .setAdminUserId(createFriendsDrinksRequest.getAdminUserId())
                    .build();
            return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent.newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.avro.FriendsDrinksEventType.CREATED)
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(r.getCreateFriendsDrinksRequest().getFriendsDrinksId().getUuid())
                            .build())
                    .setFriendsDrinksCreated(friendsDrinks)
                    .setRequestId(r.getRequestId())
                    .build();
        } else if (r.getEventType().equals(FriendsDrinksApiEventType.DELETE_FRIENDSDRINKS_REQUEST)) {
            log.info("Got delete join {}", r.getDeleteFriendsDrinksRequest().getRequestId());
            return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.avro.FriendsDrinksEventType.DELETED)
                    .setRequestId(r.getRequestId())
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(r.getDeleteFriendsDrinksRequest().getFriendsDrinksId().getUuid())
                            .build())
                    .build();
        } else if (r.getEventType().equals(FriendsDrinksApiEventType.UPDATE_FRIENDSDRINKS_REQUEST)) {
            log.info("Got update join {}", r.getUpdateFriendsDrinksRequest().getRequestId());
            UpdateFriendsDrinksRequest updateFriendsDrinksRequest = r.getUpdateFriendsDrinksRequest();

            FriendsDrinksUpdated friendsDrinks = FriendsDrinksUpdated
                    .newBuilder()
                    .setUpdateType(
                            andrewgrant.friendsdrinks.avro.UpdateType.valueOf(
                                    updateFriendsDrinksRequest.getUpdateType().name()))
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(updateFriendsDrinksRequest.getFriendsDrinksId().getUuid())
                            .build())
                    .setName(updateFriendsDrinksRequest.getName())
                    .setAdminUserId(updateFriendsDrinksRequest.getAdminUserId())
                    .build();
            return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.avro.FriendsDrinksEventType.UPDATED)
                    .setRequestId(r.getRequestId())
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(r.getUpdateFriendsDrinksRequest().getFriendsDrinksId().getUuid())
                            .build())
                    .setFriendsDrinksUpdated(friendsDrinks)
                    .build();
        } else {
            throw new RuntimeException(
                    String.format("Received unexpected event type %s", r.getEventType().name()));
        }

    }
}
