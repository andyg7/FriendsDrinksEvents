#!/usr/bin/env bash

docker exec -it broker kafka-topics --zookeeper zookeeper:2181 --list