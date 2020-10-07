package andrewgrant.friendsdrinks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import andrewgrant.friendsdrinks.api.avro.CreateFriendsDrinksRequest;
import andrewgrant.friendsdrinks.api.avro.EventType;
import andrewgrant.friendsdrinks.api.avro.FriendsDrinksInvitationReplyRequest;
import andrewgrant.friendsdrinks.api.avro.UpdateFriendsDrinksRequest;
import andrewgrant.friendsdrinks.avro.FriendsDrinksCreated;
import andrewgrant.friendsdrinks.avro.FriendsDrinksEvent;
import andrewgrant.friendsdrinks.avro.FriendsDrinksUpdated;
import andrewgrant.friendsdrinks.avro.FriendsDrinksUserAdded;

/**
 * Emits FriendsDrinks events from API results.
 */
public class RequestResponseJoiner {

    private static final Logger log = LoggerFactory.getLogger(RequestResponseJoiner.class);

    public FriendsDrinksEvent join(andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent r) {
        if (r.getEventType().equals(EventType.CREATE_FRIENDSDRINKS_REQUEST)) {
            log.info("Got create join {}", r.getCreateFriendsDrinksRequest().getRequestId());
            CreateFriendsDrinksRequest createFriendsDrinksRequest =
                    r.getCreateFriendsDrinksRequest();
            FriendsDrinksCreated friendsDrinks = FriendsDrinksCreated
                    .newBuilder()
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(createFriendsDrinksRequest.getFriendsDrinksId().getAdminUserId())
                            .setUuid(createFriendsDrinksRequest.getFriendsDrinksId().getUuid())
                            .build())
                    .setName(createFriendsDrinksRequest.getName())
                    .build();
            return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent.newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.avro.EventType.CREATED)
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(r.getCreateFriendsDrinksRequest().getFriendsDrinksId().getUuid())
                            .setAdminUserId(r.getCreateFriendsDrinksRequest().getFriendsDrinksId().getAdminUserId())
                            .build())
                    .setFriendsDrinksCreated(friendsDrinks)
                    .build();
        } else if (r.getEventType().equals(EventType.DELETE_FRIENDSDRINKS_REQUEST)) {
            log.info("Got delete join {}", r.getDeleteFriendsDrinksRequest().getRequestId());
            return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.avro.EventType.DELETED)
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(r.getDeleteFriendsDrinksRequest().getFriendsDrinksId().getUuid())
                            .setAdminUserId(r.getDeleteFriendsDrinksRequest().getFriendsDrinksId().getAdminUserId())
                            .build())
                    .build();
        } else if (r.getEventType().equals(EventType.UPDATE_FRIENDSDRINKS_REQUEST)) {
            log.info("Got update join {}", r.getUpdateFriendsDrinksRequest().getRequestId());
            UpdateFriendsDrinksRequest updateFriendsDrinksRequest = r.getUpdateFriendsDrinksRequest();

            FriendsDrinksUpdated friendsDrinks = FriendsDrinksUpdated
                    .newBuilder()
                    .setUpdateType(
                            andrewgrant.friendsdrinks.avro.UpdateType.valueOf(
                                    updateFriendsDrinksRequest.getUpdateType().name()))
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(updateFriendsDrinksRequest.getFriendsDrinksId().getAdminUserId())
                            .setUuid(updateFriendsDrinksRequest.getFriendsDrinksId().getUuid())
                            .build())
                    .setName(updateFriendsDrinksRequest.getName())
                    .build();
            return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.avro.EventType.UPDATED)
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setUuid(r.getUpdateFriendsDrinksRequest().getFriendsDrinksId().getUuid())
                            .setAdminUserId(r.getUpdateFriendsDrinksRequest().getFriendsDrinksId().getAdminUserId())
                            .build())
                    .setFriendsDrinksUpdated(friendsDrinks)
                    .build();
        } else if (r.getEventType().equals(EventType.FRIENDSDRINKS_INVITATION_REPLY_REQUEST)) {
            FriendsDrinksInvitationReplyRequest request = r.getFriendsDrinksInvitationReplyRequest();
            return andrewgrant.friendsdrinks.avro.FriendsDrinksEvent
                    .newBuilder()
                    .setEventType(andrewgrant.friendsdrinks.avro.EventType.USER_ADDED)
                    .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                            .newBuilder()
                            .setAdminUserId(request.getFriendsDrinksId().getAdminUserId())
                            .setUuid(request.getFriendsDrinksId().getUuid())
                            .build())
                    .setFriendsDrinksUserAdded(FriendsDrinksUserAdded
                            .newBuilder()
                            .setUserId(request.getUserId().getUserId())
                            .setFriendsDrinksId(andrewgrant.friendsdrinks.avro.FriendsDrinksId
                                    .newBuilder()
                                    .setUuid(request.getFriendsDrinksId().getUuid())
                                    .setAdminUserId(request.getFriendsDrinksId().getAdminUserId())
                                    .build())
                            .build())
                    .build();
        } else {
            throw new RuntimeException(
                    String.format("Received unexpected event type %s", r.getEventType().name()));
        }

    }
}
