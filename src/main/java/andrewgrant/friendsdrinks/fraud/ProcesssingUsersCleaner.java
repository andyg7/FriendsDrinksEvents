package andrewgrant.friendsdrinks.fraud;

import static andrewgrant.friendsdrinks.fraud.ValidationService.PROCESSING_USERS_STORE_NAME;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;
import andrewgrant.friendsdrinks.avro.UserId;

/**
 * Cleans state store.
 */
public class ProcesssingUsersCleaner implements
        Transformer<UserId, Long, KeyValue<UserId, User>> {

    private KeyValueStore<UserId, User> processingUsers;

    @Override
    public void init(ProcessorContext context) {
        processingUsers = (KeyValueStore<UserId, User>)
                context.getStateStore(PROCESSING_USERS_STORE_NAME);
    }

    @Override
    public KeyValue<UserId, User> transform(UserId key, Long value) {
        User user = processingUsers.get(key);
        // todo: handle null long?
        if (value > 10) {
            user.setEventType(UserEvent.REJECTED);
        } else {
            user.setEventType(UserEvent.VALIDATED);
        }
        processingUsers.put(key, null);
        return new KeyValue<>(key, user);
    }

    @Override
    public void close() {

    }
}
