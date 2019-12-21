package andrewgrant.friendsdrinks.frontend.api;

import static andrewgrant.friendsdrinks.env.Properties.loadEnvProperties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.user.UserAvro;


/**
 * Implements frontend REST API for interacting with backend.
 */
@Path("v1")
public class Service {

    private static final Logger log = LoggerFactory.getLogger(Service.class);

    public Topology buildTopology(Properties envProps,
                                  UserAvro userAvro) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String userTopicName = envProps.getProperty("user.topic.name");
        builder.table(userTopicName,
                userAvro.consumedWith());

        return builder.build();
    }

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                envProps.getProperty("frontend_application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:29092");
        return props;
    }

    @GET
    @Path("/user")
    @Produces(MediaType.TEXT_PLAIN)
    public String createUser() {
        return "Hello, world!";
    }

    public static void main(String[] args) throws IOException,
            InterruptedException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Program expects " +
                    "1) path to config 2) app port");
        }

        Properties envProps = loadEnvProperties(args[0]);
        UserAvro userAvro = new UserAvro("http://localhost:8081");
        Service service = new Service();
        Topology topology = service.buildTopology(envProps, userAvro);
        Properties streamProps = service.buildStreamsProperties(envProps);
        KafkaStreams streams = new KafkaStreams(topology, streamProps);
        int port = Integer.parseInt(args[1]);
        service.startRestService(port);
        service.startStreams(streams);
    }

    private void startRestService(int port) {
        final ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        final Server jettyServer = new Server(port);
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        try {
            jettyServer.start();
            jettyServer.join();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Listening on " + jettyServer.getURI());
    }

    private void startStreams(KafkaStreams streams)
            throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        streams.start();
        latch.await();
    }
}
