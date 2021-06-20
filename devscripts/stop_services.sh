#!/usr/bin/env bash

set -eu

docker-compose -f docker-compose.services.yml down --remove-orphans 
