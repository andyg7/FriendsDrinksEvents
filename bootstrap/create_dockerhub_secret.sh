#!/bin/zsh

set -e

export DOCKER_USERNAME=$1
export DOCKER_PASSWORD=$2

if [ -z "$DOCKER_USERNAME" ]
then
      echo "\$DOCKER_USERNAME is not set"
      exit 1
fi

if [ -z "$DOCKER_PASSWORD" ]
then
      echo "\$DOCKER_PASSWORD is empty"
      exit 1
fi

compiled_secret=$(mktemp)
cat semaphore/dockerhubsecret.yaml | envsubst | tee compiled_secret
sem create -f compiled_secret
rm -rf compiled_secret