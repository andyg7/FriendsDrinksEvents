package andrewgrant.friendsdrinks.email;

import static andrewgrant.friendsdrinks.email.CreateUserValidationService.PENDING_EMAILS_STORE_NAME;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import andrewgrant.friendsdrinks.email.avro.EmailEvent;
import andrewgrant.friendsdrinks.email.avro.EmailId;
import andrewgrant.friendsdrinks.email.avro.EventType;

/**
 * Cleans state store holding pending emails.
 */
public class PendingEmailsStateStoreCleaner implements
        Processor<EmailId, EmailEvent> {

    private KeyValueStore<String, String> pendingEmailsStore;

    @Override
    public void init(ProcessorContext context) {
        pendingEmailsStore = (KeyValueStore<String, String>)
                context.getStateStore(PENDING_EMAILS_STORE_NAME);
    }

    @Override
    public void process(EmailId emailId, EmailEvent value) {
        if (value.getEventType().equals(EventType.REJECTED) ||
                value.getEventType().equals(EventType.RESERVED)) {
            String v = pendingEmailsStore.get(value.getEmailId().getEmailAddress());
            if (v != null && v.equals(value.getUserId())) {
                pendingEmailsStore.put(value.getEmailId().getEmailAddress(), null);
            }
        } else {
            throw new RuntimeException(String.format("Received unexpected event type %s",
                    value.getEventType().toString()));
        }
    }

    @Override
    public void close() {

    }
}
