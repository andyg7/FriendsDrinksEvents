package andrewgrant.friendsdrinks.health;

import org.apache.kafka.streams.KafkaStreams;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import andrewgrant.friendsdrinks.frontend.api.HealthCheckResponseBean;

/**
 * Handler for simple Kafka streams health check.
 */
public class Handler {

    private KafkaStreams kafkaStreams;

    public Handler(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public HealthCheckResponseBean healthCheck() {
        KafkaStreams.State state = kafkaStreams.state();
        if (!state.isRunningOrRebalancing()) {
            throw new RuntimeException(String.format("State is %s", state.name()));
        }
        HealthCheckResponseBean healthCheckResponseBean = new HealthCheckResponseBean();
        healthCheckResponseBean.setStatus("HEALTHY");
        return healthCheckResponseBean;
    }
}
