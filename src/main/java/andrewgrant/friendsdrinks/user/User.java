package andrewgrant.friendsdrinks.user;

import andrewgrant.friendsdrinks.user.avro.UserId;

/**
 * POJO class for User.
 */
public class User {
    private UserId userId;
    private String email;
    private String requestId;

    public User(UserId userId, String email, String requestId) {
        this.userId = userId;
        this.email = email;
        this.requestId = requestId;
    }

    public UserId getUserId() {
        return userId;
    }

    public String getEmail() {
        return email;
    }

    public String getRequestId() {
        return requestId;
    }
}
