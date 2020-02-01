package andrewgrant.friendsdrinks;

import static andrewgrant.friendsdrinks.env.Properties.load;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.io.IOException;
import java.util.Properties;

public class Service {

    private Topology buildTopology(Properties envProps) {
        StreamsBuilder builder = new StreamsBuilder();
        String friendsDrinksTopicName = envProps.getProperty("friendsdrinks.topic.name");
        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        Properties envProperties = load(args[0]);

    }
}
