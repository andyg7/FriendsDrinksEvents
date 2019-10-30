package andrewgrant.friendsdrinks.fraud;

import static andrewgrant.friendsdrinks.fraud.ValidationService.PROCESSING_USERS_STORE_NAME;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserId;

/**
 * Populates state store.
 */
public class ProcessingUsersPopulator implements
        Transformer<UserId, User, KeyValue<UserId, User>> {

    private KeyValueStore<UserId, User> processingUsers;

    @Override
    public void init(ProcessorContext context) {
        processingUsers = (KeyValueStore<UserId, User>)
                context.getStateStore(PROCESSING_USERS_STORE_NAME);
    }

    @Override
    public KeyValue<UserId, User> transform(UserId key, User value) {
        processingUsers.put(key, value);
        return new KeyValue<>(key, value);
    }

    @Override
    public void close() {

    }
}

