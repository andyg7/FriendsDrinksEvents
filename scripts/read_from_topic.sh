#!/usr/bin/env bash

TOPIC=$1

docker exec -it schema-registry /usr/bin/kafka-avro-console-consumer --topic ${TOPIC} --bootstrap-server broker:9092  --from-beginning


