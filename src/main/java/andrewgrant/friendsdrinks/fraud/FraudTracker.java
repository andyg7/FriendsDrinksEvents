package andrewgrant.friendsdrinks.fraud;

import andrewgrant.friendsdrinks.avro.User;

/**
 * Class that represents a user request.
 */
public class FraudTracker {
    private User user;
    private Long count;

    public FraudTracker(User user, Long count) {
        this.user = user;
        this.count = count;
    }

    public User getUser() {
        return user;
    }

    public Long getCount() {
        return count;
    }
}

