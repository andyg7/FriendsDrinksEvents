package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.avro.FriendsDrinksEvent;

/**
 * Holds admin user ID and event type.
 */
public class AdminAndEvent {
    private String adminUserId;
    private FriendsDrinksEvent eventType;

    public AdminAndEvent(String adminUserId, FriendsDrinksEvent eventType) {
        this.adminUserId = adminUserId;
        this.eventType = eventType;
    }

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }

    public FriendsDrinksEvent getFriendsDrinksEvent() {
        return eventType;
    }

    public void setFriendsDrinksEvent(FriendsDrinksEvent eventType) {
        this.eventType = eventType;
    }

}
