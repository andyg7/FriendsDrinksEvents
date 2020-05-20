#!/usr/bin/env bash

TOPIC=$1

kubectl exec -it schema-registry -- /usr/bin/kafka-avro-console-consumer --topic ${TOPIC} \
--bootstrap-server kafka-cs:9092  --from-beginning

