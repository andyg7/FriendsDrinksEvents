package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.email.UserEmailValidatorService.PENDING_EMAILS_STORE_NAME;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.EmailEvent;
import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;

/**
 * Validates email request.
 */
public class EmailValidator implements Transformer<String, EmailRequest, KeyValue<String, User>> {

    private KeyValueStore<String, String> pendingEmailsStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        pendingEmailsStore = (KeyValueStore<String, String>) context
                .getStateStore(PENDING_EMAILS_STORE_NAME);
    }

    @Override
    public KeyValue<String, User> transform(final String str, final EmailRequest emailRequest) {
        String requestedEmail = emailRequest.getUser().getEmail();
        if (pendingEmailsStore.get(requestedEmail) != null) {
            User user = emailRequest.getUser();
            user.setEventType(UserEvent.REJECTED);
            user.setErrorCode(ErrorCode.PENDING.name());
            return new KeyValue<>(str, user);
        }
        Email email = emailRequest.getEmail();
        if (email == null) {
            User user = emailRequest.getUser();
            // Add email address to pending state store
            pendingEmailsStore.put(requestedEmail, user.getUserId());
            user.setEventType(UserEvent.VALIDATED);
            return new KeyValue<>(str, user);
        } else if (email.getEventType().equals(EmailEvent.RESERVED)) {
            User user = emailRequest.getUser();
            user.setEventType(UserEvent.REJECTED);
            user.setErrorCode(ErrorCode.EXISTS.name());
            return new KeyValue<>(str, user);
        }

        throw new RuntimeException();
    }

    @Override
    public void close() {
    }
}
