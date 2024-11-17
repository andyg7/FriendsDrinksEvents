package andrewgrant.friendsdrinks.user;

import static andrewgrant.friendsdrinks.streamsconfig.FilePropsLoader.load;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import andrewgrant.friendsdrinks.avro.*;
import andrewgrant.friendsdrinks.streamsconfig.SharedConfigSetter;
import com.sun.net.httpserver.HttpServer;

/**
 * Contains user service.
 */
public class Service {

    private static final Logger log = LoggerFactory.getLogger(Service.class);

    private Properties envProps;
    private AvroBuilder avroBuilder;

    public Service(Properties envProps, AvroBuilder avroBuilder) {
        this.envProps = envProps;
        this.avroBuilder = avroBuilder;
    }

    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<UserId, UserEvent> userEvents = builder.stream(envProps.getProperty(TopicNameConfigKey.USER_EVENT),
                Consumed.with(avroBuilder.userIdSerde(), avroBuilder.userEventSerde()));
        userEvents.groupByKey(Grouped.with(avroBuilder.userIdSerde(), avroBuilder.userEventSerde()))
                .aggregate(
                        () -> UserStateAggregate.newBuilder().build(),
                        (aggKey, newValue, aggValue) -> {
                            if (newValue.getEventType().equals(UserEventType.LOGGED_IN)) {
                                UserStateAggregate userStateAggregate = UserStateAggregate
                                        .newBuilder(aggValue)
                                        .setUserState(UserState
                                                .newBuilder()
                                                .setStatus(UserStatus.ACTIVE)
                                                .setUserId(newValue.getUserLoggedIn().getUserId())
                                                .setFirstName(newValue.getUserLoggedIn().getFirstName())
                                                .setLastName(newValue.getUserLoggedIn().getLastName())
                                                .setEmail(newValue.getUserLoggedIn().getEmail())
                                                .build())
                                        .build();
                                return userStateAggregate;
                            } else if (newValue.getEventType().equals(UserEventType.LOGGED_OUT)) {
                                return aggValue;
                            } else if (newValue.getEventType().equals(UserEventType.SIGNED_OUT_SESSION_EXPIRED)) {
                                return aggValue;
                            } else if (newValue.getEventType().equals(UserEventType.DELETED)) {
                                UserState userState = aggValue.getUserState();
                                userState.setStatus(UserStatus.DELETED);
                                return UserStateAggregate.newBuilder().setUserState(userState).build();
                            } else {
                                throw new RuntimeException(String.format("Unknown event type %s", newValue.getEventType().name()));
                            }
                        },
                        Materialized.with(avroBuilder.userIdSerde(), avroBuilder.userStateAggregateSerde())
                ).toStream().mapValues(value -> {
            if (value == null) {
                return null;
            } else {
                return value.getUserState();
            }
        })
                .to(envProps.getProperty(TopicNameConfigKey.USER_STATE), Produced.with(avroBuilder.userIdSerde(), avroBuilder.userStateSerde()));

        return builder.build();
    }

    public Properties buildStreamProperties(Properties envProps) {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("user.application.id"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        streamProps.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        if (envProps.getProperty("streams.dir") != null) {
            streamProps.put(StreamsConfig.STATE_DIR_CONFIG, envProps.getProperty("streams.dir"));
        }
        return streamProps;
    }

    public static void main(String[] args) throws IOException {
        Properties envProps = load(args[0]);
        String schemaRegistryUrl = envProps.getProperty("schema.registry.url");
        Service service = new Service(envProps, new AvroBuilder(schemaRegistryUrl));
        Topology topology = service.buildTopology();
        Properties streamProps = service.buildStreamProperties(envProps);
        streamProps = SharedConfigSetter.addSharedConfig(streamProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProps);
        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception {}", exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        log.info("Starting Service application...");

        HttpServer healthCheckServer = andrewgrant.friendsdrinks.health.Server.buildServer(8080, kafkaStreams);


        final CountDownLatch latch = new CountDownLatch(2);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread("health-check-shutdown-hook") {
            @Override
            public void run() {
                andrewgrant.friendsdrinks.health.Server.stop(healthCheckServer);
                latch.countDown();
            }
        });

        kafkaStreams.start();
        andrewgrant.friendsdrinks.health.Server.start(healthCheckServer);
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
