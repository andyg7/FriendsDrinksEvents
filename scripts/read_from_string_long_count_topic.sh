#!/usr/bin/env bash

docker exec -it broker /usr/bin/kafka-console-consumer --topic friendsdrinks_request_application-internal_request_service_friendsdrinks_count_tracker-changelog   --bootstrap-server broker:9092 --from-beginning --property print.value=true --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
    --property print.value=true --property print.key=true \
     --bootstrap-server broker:9092 --from-beginning