package andrewgrant.friendsdrinks.user;

import andrewgrant.friendsdrinks.avro.EventType;
import andrewgrant.friendsdrinks.avro.UserEvent;
import andrewgrant.friendsdrinks.avro.UserId;

/**
 *
 */
public class UserValidation {

    private UserEvent userEvent;
    private Long count;
    private Long limit;

    public UserValidation(UserEvent userEvent, Long count, Long limit) {
        this.userEvent = userEvent;
        this.count = count;
        this.limit = limit;
    }

    public UserId getUserId() {
       if (userEvent.getEventType().equals(EventType.VALIDATED)) {
           return userEvent.getUserValidated().getUserId();
       } else {
          return userEvent.getUserRejected().getUserId();
       }
    }

    public UserEvent getUserEvent() {
        return userEvent;
    }

    public Long getCount() {
        return count;
    }

    public boolean isValidated() {
        if (count >= limit) {
            return true;
        } else {
            return false;
        }
    }
}

