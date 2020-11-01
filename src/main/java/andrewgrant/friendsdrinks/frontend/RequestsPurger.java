package andrewgrant.friendsdrinks.frontend;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import andrewgrant.friendsdrinks.api.avro.ApiEvent;

/**
 * Purges old requests.
 */
public class RequestsPurger implements Transformer<String, ApiEvent, KeyValue<String, List<String>>> {

    public static final String RESPONSES_PENDING_DELETION = "responses-pending-deletion";

    private static final Logger log = LoggerFactory.getLogger(RequestsPurger.class);

    private KeyValueStore<String, ApiEvent> stateStore;

    @Override
    public void init(ProcessorContext context) {
        stateStore = (KeyValueStore) context.getStateStore(RESPONSES_PENDING_DELETION);
        log.info("Set up {}", this.getClass());
    }

    @Override
    public KeyValue<String, List<String>> transform(String key, ApiEvent value) {
        log.info("Received request {}", key);
        if (value == null) {
            log.info("Deleting {}", key);
            stateStore.delete(key);
        } else {
            log.info("Storing {}", key);
            stateStore.put(key, value);
        }
        long numEntries = stateStore.approximateNumEntries();
        log.info("State store approx num entries is {}", numEntries);
        if (numEntries > 2) {
            final KeyValueIterator<String, ApiEvent> iterator = stateStore.all();
            List<String> requestsToPurge = new ArrayList<>();
            while (iterator.hasNext()) {
                final KeyValue<String, ApiEvent> record = iterator.next();
                requestsToPurge.add(record.key);
            }
            iterator.close();
            StringBuilder sb = new StringBuilder();
            for (String requestToPurge : requestsToPurge) {
                sb.append(requestToPurge);
                sb.append(" ");
            }
            log.info("Purging: {}", sb.toString());
            return new KeyValue<>(key, requestsToPurge);
        } else {
            return null;
        }
    }

    @Override
    public void close() {

    }
}
