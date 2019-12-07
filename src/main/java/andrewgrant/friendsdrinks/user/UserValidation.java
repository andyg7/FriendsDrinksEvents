package andrewgrant.friendsdrinks.user;

/**
 *
 */
public class UserValidation {

    private User user;
    private Long count;
    private Long limit;

    public UserValidation(User user, Long count, Long limit) {
        this.user = user;
        this.count = count;
        this.limit = limit;
    }

    public User getUser() {
        return user;
    }

    public boolean isValidated() {
        if (count >= limit) {
            return true;
        } else {
            return false;
        }
    }
}

