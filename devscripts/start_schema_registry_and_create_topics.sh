#!/usr/bin/env bash

set -eu

docker-compose -f docker-compose.schema-registry.yml --project-name schema-registry up
