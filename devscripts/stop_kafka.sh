#!/usr/bin/env bash

set -eu

docker-compose -f docker-compose.kafka.yml --project-name friendsdrinks down
