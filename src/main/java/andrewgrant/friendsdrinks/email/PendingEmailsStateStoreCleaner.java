package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.email.ValidationService.PENDING_EMAILS_STORE_NAME;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;
import andrewgrant.friendsdrinks.email.avro.EventType;

/**
 * Cleans state store holding pending emails.
 */
public class PendingEmailsStateStoreCleaner implements
        Transformer<EmailId, EmailEvent, KeyValue<EmailId, EmailEvent>> {

    private KeyValueStore<String, String> pendingEmailsStore;

    @Override
    public void init(ProcessorContext context) {
        pendingEmailsStore = (KeyValueStore<String, String>)
                context.getStateStore(PENDING_EMAILS_STORE_NAME);
    }

    @Override
    public KeyValue<EmailId, EmailEvent> transform(EmailId emailId, EmailEvent value) {
        if (value.getEventType().equals(EventType.REJECTED)) {
            String v = pendingEmailsStore.get(value.getEmailId().getEmailAddress());
            if (v != null && v.equals(value.getUserId())) {
                pendingEmailsStore.put(value.getEmailId().getEmailAddress(), null);
            }
        }
        return new KeyValue<>(emailId, value);
    }

    @Override
    public void close() {

    }
}
