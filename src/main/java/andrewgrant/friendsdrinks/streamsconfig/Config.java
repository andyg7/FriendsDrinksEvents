package andrewgrant.friendsdrinks.streamsconfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 *
 */
public class Config {

    private static final String STREAMS_GROUP_INSTANCE_ID_ENV_VAR = "STREAMS_GROUP_INSTANCE_ID";

    public static java.util.Properties addSharedConfig(java.util.Properties streamsConfig) {
        streamsConfig.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), "120000");
        streamsConfig.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), "15000");
        if (System.getenv(STREAMS_GROUP_INSTANCE_ID_ENV_VAR) != null) {
            streamsConfig.put(StreamsConfig.consumerPrefix(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG),
                    System.getenv(STREAMS_GROUP_INSTANCE_ID_ENV_VAR));
        }
        return streamsConfig;
    }
}
