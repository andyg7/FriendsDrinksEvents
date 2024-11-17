#!/usr/bin/env bash

set -eu

docker-compose -f docker-compose/docker-compose.kafka.yml --project-name kafka down
