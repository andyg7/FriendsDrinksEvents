package andrewgrant.friendsdrinks.frontend;

import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import andrewgrant.friendsdrinks.api.avro.*;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;


/**
 * Builds serdes.
 */
public class AvroBuilder {

    private SchemaRegistryClient registryClient;
    private String registryUrl;

    public AvroBuilder(String registryUrl) {
        this.registryUrl = registryUrl;
        registryClient = null;
    }

    public AvroBuilder(String registryUrl, SchemaRegistryClient registryClient) {
        this.registryUrl = registryUrl;
        this.registryClient = registryClient;
    }


    public SpecificAvroSerde<ApiEvent> apiEventSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.ApiEvent> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<FriendsDrinksEvent> friendsDrinksEventSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksEvent> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }


    public Serializer<ApiEvent> apiEventSerializer() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.ApiEvent> serde = apiEventSerde();
        return serde.serializer();
    }

    public SpecificAvroSerde<CreateFriendsDrinksRequest> createFriendsDrinksRequestSerde() {
        SpecificAvroSerde<CreateFriendsDrinksRequest> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<UpdateFriendsDrinksRequest> updateFriendsDrinksRequestSerde() {
        SpecificAvroSerde<UpdateFriendsDrinksRequest> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }


    public SpecificAvroSerde<DeleteFriendsDrinksRequest> deleteFriendsDrinksRequestSerde() {
        SpecificAvroSerde<DeleteFriendsDrinksRequest> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<FriendsDrinksInvitationRequest> friendsDrinksInvitationRequestSerde() {
        SpecificAvroSerde<FriendsDrinksInvitationRequest> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<FriendsDrinksInvitationResponse> friendsDrinksInvitationResponseSerde() {
        SpecificAvroSerde<FriendsDrinksInvitationResponse> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<FriendsDrinksInvitationReplyRequest> friendsDrinksInvitationReplyRequestSerde() {
        SpecificAvroSerde<FriendsDrinksInvitationReplyRequest> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<FriendsDrinksInvitationReplyResponse> friendsDrinksInvitationReplyResponseSerde() {
        SpecificAvroSerde<FriendsDrinksInvitationReplyResponse> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> friendsDrinksIdSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksId> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, true);
        return serde;
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksState> friendsDrinksStateSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksState> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksIdList> friendsDrinksIdListSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksIdList> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.UserState> userStateSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.UserState> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.UserStateList> userStateListSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.UserStateList> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }


    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksEnrichedMembershipState>
    friendsDrinksEnrichedMembershipStateSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksEnrichedMembershipState> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksAggregate> friendsDrinksAggregateSerdes() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksAggregate> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipId> friendsDrinksMembershipIdSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipId> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, true);
        return serde;
    }

    public SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipEvent> friendsDrinksMembershipEventSerde() {
        SpecificAvroSerde<andrewgrant.friendsdrinks.api.avro.FriendsDrinksMembershipEvent> serde;
        if (registryClient != null) {
            serde = new SpecificAvroSerde<>(registryClient);
        } else {
            serde = new SpecificAvroSerde<>();
        }
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        serde.configure(config, false);
        return serde;
    }
}
