#!/usr/bin/env bash

set -eu

docker-compose -f dockercompose/docker-compose.services.yml --project-name app down
