package andrewgrant.friendsdrinks.fraud;

import andrewgrant.friendsdrinks.avro.UserEvent;

/**
 * Class that represents a user request.
 */
public class FraudTracker {
    private UserEvent userEvent;
    private Long count;

    public FraudTracker(UserEvent userEvent, Long count) {
        this.userEvent = userEvent;
        this.count = count;
    }

    public UserEvent getUserEvent() {
        return userEvent;
    }

    public Long getCount() {
        return count;
    }
}

