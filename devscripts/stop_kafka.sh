#!/usr/bin/env bash

set -eu

docker-compose -f dockercompose/docker-compose.kafka.yml --project-name kafka down
