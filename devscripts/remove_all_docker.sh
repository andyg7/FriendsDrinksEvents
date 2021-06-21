#!/usr/bin/env bash

set -eu

docker rm -f $(docker ps -a -q)
