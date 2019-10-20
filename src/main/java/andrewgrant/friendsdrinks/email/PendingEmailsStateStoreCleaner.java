package andrewgrant.friendsdrinks.email;

import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import static andrewgrant.friendsdrinks.email.UserEmailValidatorService.PENDING_EMAILS_STORE_NAME;

public class PendingEmailsStateStoreCleaner implements
        Transformer<String, User, KeyValue<String, User>> {

    private KeyValueStore<String, String> pendingEmailsStore;

    @Override
    public void init(ProcessorContext context) {
        pendingEmailsStore = (KeyValueStore<String, String>) context
                .getStateStore(PENDING_EMAILS_STORE_NAME);
    }

    @Override
    public KeyValue<String, User> transform(String key, User value) {
        if (value.getEventType().equals(UserEvent.REJECTED)) {
            pendingEmailsStore.put(value.getEmail(), null);
        }
        return new KeyValue<>(key, value);
    }

    @Override
    public void close() {

    }
}
