#!/usr/bin/env bash

docker exec -it broker /usr/bin/kafka-console-consumer --topic friendsdrinks-invitation-request-application-pending-friendsdrinks-membership-requests-store-changelog --bootstrap-server broker:9092 --from-beginning --property print.value=true --property print.key=true \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property print.value=true --property print.key=false \
    --from-beginning