package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.EmailEvent;
import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import static andrewgrant.friendsdrinks.UserDetailsService.PENDING_EMAILS_STORE_NAME;

public class EmailValidator implements
        Transformer<String, EmailRequest, KeyValue<String, User>> {

    private KeyValueStore<String, String> pendingEmailsStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        pendingEmailsStore = (KeyValueStore<String, String>) context
                .getStateStore(PENDING_EMAILS_STORE_NAME);
    }

    @Override
    public KeyValue<String, User> transform(final String str, final EmailRequest emailRequest) {
        Email email = emailRequest.getEmail();
        if (pendingEmailsStore.get(email.getEmail()) != null) {
            User user = emailRequest.getUser();
            user.setEventType(UserEvent.REJECTED);
            return new KeyValue<>(str, user);
        } else if (email == null || email.getEventType().equals(EmailEvent.RECLAIMED)) {
            User user = emailRequest.getUser();
            // Add email address to pending state store
            pendingEmailsStore.put(email.getEmail(), user.getUserId());
            user.setEventType(UserEvent.VALIDATED);
            return new KeyValue<>(str, user);
        } if (email.getEventType().equals(EmailEvent.RESERVED)) {
            // Remove email address from pending state store as its been reserved in the emails topic
            pendingEmailsStore.put(email.getEmail(), null);
            User user = emailRequest.getUser();
            user.setEventType(UserEvent.REJECTED);
            return new KeyValue<>(str, user);
        }

        throw new RuntimeException();
    }

    @Override
    public void close() {
    }
}
