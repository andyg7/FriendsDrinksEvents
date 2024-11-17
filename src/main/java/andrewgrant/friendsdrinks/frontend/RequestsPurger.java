package andrewgrant.friendsdrinks.frontend;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import andrewgrant.friendsdrinks.avro.ApiEvent;

/**
 * Purges old requests.
 */
public class RequestsPurger implements Processor<String, ApiEvent, String, List<String>> {

    public static final String RESPONSES_PENDING_DELETION = "responses-pending-deletion";

    private static final Logger log = LoggerFactory.getLogger(RequestsPurger.class);

    private KeyValueStore<String, ApiEvent> stateStore;

    private org.apache.kafka.streams.processor.api.ProcessorContext<String, List<String>> context;

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, List<String>> context) {
        stateStore = (KeyValueStore) context.getStateStore(RESPONSES_PENDING_DELETION);
        log.info("Set up {}", this.getClass());
        this.context = context;
    }

    @Override
    public void process(Record<String, ApiEvent> record) {
        String key = record.key();
        ApiEvent value = record.value();
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
                final KeyValue<String, ApiEvent> next = iterator.next();
                requestsToPurge.add(next.key);
            }
            iterator.close();
            StringBuilder sb = new StringBuilder();
            for (String requestToPurge : requestsToPurge) {
                sb.append(requestToPurge);
                sb.append(" ");
            }
            log.info("Purging: {}", sb.toString());
            context.forward(new Record<String, List<String>>(key, requestsToPurge, System.currentTimeMillis()));
        } else {
            context.forward(null);
        }
    }

    @Override
    public void close() {

    }
}
