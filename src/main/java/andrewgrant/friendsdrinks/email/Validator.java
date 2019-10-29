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
public class Validator implements Transformer<EmailId, Request, KeyValue<EmailId, User>> {

    private KeyValueStore<String, String> pendingEmailsStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        pendingEmailsStore = (KeyValueStore<String, String>) context
                .getStateStore(PENDING_EMAILS_STORE_NAME);
    }

    @Override
    public KeyValue<EmailId, User> transform(final EmailId emailId,
                                             final Request request) {
        String requestedEmail = request.getUserRequest().getEmail();
        if (pendingEmailsStore.get(requestedEmail) != null) {
            User user = request.getUserRequest();
            user.setEventType(UserEvent.REJECTED);
            user.setErrorCode(ErrorCode.PENDING.name());
            return new KeyValue<>(emailId, user);
        }
        Email email = request.getCurrEmailState();
        if (email == null) {
            User user = request.getUserRequest();
            // Add email address to pending state store
            pendingEmailsStore.put(requestedEmail, user.getUserId().getId());
            user.setEventType(UserEvent.VALIDATED);
            return new KeyValue<>(emailId, user);
        } else if (email.getEventType().equals(EmailEvent.RESERVED)) {
            User user = request.getUserRequest();
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
