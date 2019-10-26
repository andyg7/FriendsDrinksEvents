package andrewgrant.friendsdrinks.email;

import static org.junit.Assert.*;

import static andrewgrant.friendsdrinks.email.Config.TEST_CONFIG_FILE;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * Tests for validation.
 */
public class UserEmailValidatorServiceTest {

    @Test
    public void testValidation() throws IOException {
        UserEmailValidatorService validatorService = new UserEmailValidatorService();
        Properties envProps = validatorService.loadEnvProperties(TEST_CONFIG_FILE);
        Topology topology = validatorService.buildTopology(envProps);

        Properties streamProps = validatorService.buildStreamsProperties(envProps);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamProps);
    }

}
