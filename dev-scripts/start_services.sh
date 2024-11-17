#!/usr/bin/env bash

set -eu

docker-compose -f docker-compose/docker-compose.services.yml --project-name app up
