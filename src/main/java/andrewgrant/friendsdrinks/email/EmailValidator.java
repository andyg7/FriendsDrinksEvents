package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.email.ValidationService.PENDING_EMAILS_STORE_NAME;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import andrewgrant.friendsdrinks.avro.*;

/**
 * Validates email request.
 */
public class EmailValidator implements Transformer<EmailId, EmailRequest, KeyValue<EmailId, User>> {

    private KeyValueStore<String, String> pendingEmailsStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        pendingEmailsStore = (KeyValueStore<String, String>) context
                .getStateStore(PENDING_EMAILS_STORE_NAME);
    }

    @Override
    public KeyValue<EmailId, User> transform(final EmailId emailId,
                                             final EmailRequest emailRequest) {
        String requestedEmail = emailRequest.getUserRequest().getEmail();
        if (pendingEmailsStore.get(requestedEmail) != null) {
            User user = emailRequest.getUserRequest();
            user.setEventType(UserEvent.REJECTED);
            user.setErrorCode(ErrorCode.PENDING.name());
            return new KeyValue<>(emailId, user);
        }
        Email email = emailRequest.getCurrEmailState();
        if (email == null) {
            User user = emailRequest.getUserRequest();
            // Add email address to pending state store
            pendingEmailsStore.put(requestedEmail, user.getUserId().getId());
            user.setEventType(UserEvent.VALIDATED);
            return new KeyValue<>(emailId, user);
        } else if (email.getEventType().equals(EmailEvent.RESERVED)) {
            User user = emailRequest.getUserRequest();
            user.setEventType(UserEvent.REJECTED);
            user.setErrorCode(ErrorCode.EXISTS.name());
            return new KeyValue<>(emailId, user);
        }

        throw new RuntimeException();
    }

    @Override
    public void close() {
    }
}
