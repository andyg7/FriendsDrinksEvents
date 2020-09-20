package andrewgrant.friendsdrinks;

import andrewgrant.friendsdrinks.avro.EventType;

/**
 * Holds admin user ID and event type.
 */
public class AdminAndEventType {
    private String adminUserId;
    private EventType eventType;

    public AdminAndEventType(String adminUserId, EventType eventType) {
        this.adminUserId = adminUserId;
        this.eventType = eventType;
    }

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

}
