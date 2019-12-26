package andrewgrant.friendsdrinks.frontend.cli;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import andrewgrant.friendsdrinks.user.UserAvro;
import andrewgrant.friendsdrinks.user.avro.CreateUserRequest;
import andrewgrant.friendsdrinks.user.avro.EventType;
import andrewgrant.friendsdrinks.user.avro.UserEvent;
import andrewgrant.friendsdrinks.user.avro.UserId;


/**
 * Bootstraps schemas.
 */
public class CreateUser {

    public static void main(String[] args) throws IOException,
            ExecutionException,
            InterruptedException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Program expects " +
                    "1) path to config 2) email");
        }
        Properties envProps = loadEnvProperties(args[0]);
        UserAvro userAvro = new UserAvro("http://localhost:8081");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:29092");

        KafkaProducer<UserId, UserEvent> producer =
                new KafkaProducer<>(
                        props,
                        userAvro.userIdSerializer(),
                        userAvro.userEventSerializer());
        UserId userId = UserId.newBuilder()
                .setId(UUID.randomUUID().toString())
                .build();
        CreateUserRequest request = CreateUserRequest.newBuilder()
                .setEmail(args[1])
                .setUserId(userId)
                .setRequestId(UUID.randomUUID().toString())
                .build();
        UserEvent userEvent = UserEvent.newBuilder()
                .setEventType(EventType.CREATE_USER_REQUEST)
                .setCreateUserRequest(request)
                .build();
        final String userTopic = envProps.getProperty("user.topic.name");
        ProducerRecord<UserId, UserEvent> record =
                new ProducerRecord<>(
                        userTopic,
                        userEvent.getCreateUserRequest().getUserId(),
                        userEvent);
        RecordMetadata recordMetadata = producer.send(record).get();
        System.out.println(recordMetadata.toString());
    }
}
