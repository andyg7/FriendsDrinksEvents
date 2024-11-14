#!/usr/bin/env bash

docker exec -it broker kafka-topics --bootstrap-server broker:9092 --list
