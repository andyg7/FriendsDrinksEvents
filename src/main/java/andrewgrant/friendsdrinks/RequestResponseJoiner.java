package andrewgrant.friendsdrinks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import andrewgrant.friendsdrinks.api.avro.CreateFriendsDrinksRequest;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksEventType;
import andrewgrant.friendsdrinks.api.avro.UpdateFriendsDrinksRequest;
import andrewgrant.friendsdrinks.avro.FriendsDrinksCreated;
import andrewgrant.friendsdrinks.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.avro.FriendsDrinksUpdated;

/**
 * Emits FriendsDrinks events from API results.
 */
public class RequestResponseJoiner {

    private static final Logger log = LoggerFactory.getLogger(RequestResponseJoiner.class);

    public FriendsDrinksEvent join(andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent r) {
        if (r.getEventType().equals(FriendsDrinksEventType.CREATE_FRIENDSDRINKS_REQUEST)) {
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
                    .setEventType(andrewgrant.friendsdrinks.avro.EventType.CREATED)
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(r.getCreateFriendsDrinksRequest().getFriendsDrinksId().getUuid())
                            .build())
                    .setFriendsDrinksCreated(friendsDrinks)
                    .setRequestId(r.getRequestId())
                    .build();
        } else if (r.getEventType().equals(FriendsDrinksEventType.DELETE_FRIENDSDRINKS_REQUEST)) {
            log.info("Got delete join {}", r.getDeleteFriendsDrinksRequest().getRequestId());
            return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.avro.EventType.DELETED)
                    .setRequestId(r.getRequestId())
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(r.getDeleteFriendsDrinksRequest().getFriendsDrinksId().getUuid())
                            .build())
                    .build();
        } else if (r.getEventType().equals(FriendsDrinksEventType.UPDATE_FRIENDSDRINKS_REQUEST)) {
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
                    .setEventType(andrewgrant.friendsdrinks.avro.EventType.UPDATED)
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
