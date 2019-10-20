package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.User;

public class EmailRequest {
    private User user;
    private Email email;

    public EmailRequest(User user, Email email) {
        this.user = user;
        this.email = email;
    }

    public User getUser() {
        return user;
    }

    public Email getEmail() {
        return email;
    }
}
