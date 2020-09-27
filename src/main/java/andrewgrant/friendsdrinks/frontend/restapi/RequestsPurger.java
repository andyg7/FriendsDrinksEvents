package andrewgrant.friendsdrinks.frontend.restapi;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

import andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent;

/**
 * Purges old requests.
 */
public class RequestsPurger implements Transformer<String, FriendsDrinksEvent, KeyValue<String, List<String>>> {

    public static final String PENDING_RESPONSES = "pending-responses";

    private KeyValueStore<String, FriendsDrinksEvent> stateStore;

    @Override
    public void init(ProcessorContext context) {
        stateStore = (KeyValueStore) context.getStateStore(PENDING_RESPONSES);
    }

    @Override
    public KeyValue<String, List<String>> transform(String key, FriendsDrinksEvent value) {
        if (value == null) {
            stateStore.delete(key);
        } else {
            stateStore.put(key, value);
        }
        final KeyValueIterator<String, FriendsDrinksEvent> iterator = stateStore.all();
        List<String> requestsToPurge = new ArrayList<>();
        while (iterator.hasNext()) {
            final KeyValue<String, FriendsDrinksEvent> record = iterator.next();
            requestsToPurge.add(record.key);
        }
        return new KeyValue<>(key, requestsToPurge);
    }

    @Override
    public void close() {

    }
}
