package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.email.UserEmailValidatorService.PENDING_EMAILS_STORE_NAME;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.EmailEvent;
import andrewgrant.friendsdrinks.avro.EmailId;

/**
 * Cleans state store holding pending emails.
 */
public class PendingEmailsStateStoreCleaner implements
        Transformer<EmailId, Email, KeyValue<EmailId, Email>> {

    private KeyValueStore<String, String> pendingEmailsStore;

    @Override
    public void init(ProcessorContext context) {
        pendingEmailsStore = (KeyValueStore<String, String>)
                context.getStateStore(PENDING_EMAILS_STORE_NAME);
    }

    @Override
    public KeyValue<EmailId, Email> transform(EmailId emailId, Email value) {
        if (value.getEventType().equals(EmailEvent.REJECTED) &&
                pendingEmailsStore.get(value.getEmailId().getEmailAddress())
                        .equals(value.getUserId())) {
            pendingEmailsStore.put(value.getEmailId().getEmailAddress(), null);
        }
        return new KeyValue<>(emailId, value);
    }

    @Override
    public void close() {

    }
}
