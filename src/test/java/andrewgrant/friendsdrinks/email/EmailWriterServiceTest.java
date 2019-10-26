package andrewgrant.friendsdrinks.email;

import static org.junit.Assert.*;

import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import andrewgrant.friendsdrinks.avro.Email;
import andrewgrant.friendsdrinks.avro.EmailEvent;
import andrewgrant.friendsdrinks.avro.User;
import andrewgrant.friendsdrinks.avro.UserEvent;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import andrewgrant.friendsdrinks.user.UserAvro;

/**
 * Tests for EmailWriterService.
 */
public class EmailWriterServiceTest {


    /**
     * Integration test that requires kafka and schema registry to be running and so is ignored
     * by default.
     * @throws IOException
     */
    @Ignore
    @Test
    public void testValidate() throws IOException {
        EmailWriterService emailWriterService = new EmailWriterService();
        Properties envProps = emailWriterService.loadEnvProperties(TEST_CONFIG_FILE);
        Topology topology = emailWriterService.buildTopology(envProps);

        Properties streamProps = emailWriterService.buildStreamsProperties(envProps);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamProps);

        Serializer<String> keySerializer = Serdes.String().serializer();
        SpecificAvroSerializer<User> userSerializer = UserAvro.serializer(envProps);

        ConsumerRecordFactory<String, User> inputFactory =
                new ConsumerRecordFactory<>(keySerializer, userSerializer);

        List<User> input = new ArrayList<>();
        input.add(
                User.newBuilder()
                        .setRequestId("1")
                        .setUserId("userid1")
                        .setEmail("userid1@test.com")
                        .setEventType(UserEvent.VALIDATED).build());
        input.add(
                User.newBuilder()
                        .setRequestId("2")
                        .setUserId("userid2")
                        .setEmail("userid2@test.com")
                        .setEventType(UserEvent.REJECTED).build());

        final String userTopic = envProps.getProperty("user.topic.name");
        for (User user : input) {
            testDriver.pipeInput(inputFactory.create(userTopic, user.getUserId(), user));
        }

        final String emailTopic = envProps.getProperty("email.topic.name");
        Deserializer<String> keyDeserializer = Serdes.String().deserializer();
        SpecificAvroDeserializer<Email> emailDeserializer = EmailAvro
                .deserializer(envProps);

        List<Email> output = new ArrayList<>();
        while (true) {
            ProducerRecord<String, Email> emailRecord =
                    testDriver.readOutput(emailTopic, keyDeserializer, emailDeserializer);
            if (emailRecord != null) {
                output.add(emailRecord.value());
            } else {
                break;
            }
        }

        assertEquals(2, output.size());
        Email email1 = output.get(0);
        assertEquals(EmailEvent.RESERVED, email1.getEventType());
        Email email2 = output.get(1);
        assertEquals(EmailEvent.REJECTED, email2.getEventType());
    }

}
