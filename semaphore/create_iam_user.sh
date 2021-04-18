#!/bin/zsh

set -e

export AWS_ACCESS_KEY_ID=$1
export AWS_SECRET_ACCESS_KEY=$2

if [ -z "$AWS_ACCESS_KEY_ID" ]
then
      echo "\$AWS_ACCESS_KEY_ID is not set"
      exit 1
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]
then
      echo "\$AWS_SECRET_ACCESS_KEY is empty"
      exit 1
fi

compiled_secret=$(mktemp)
cat semaphore/dockerhubsecret.yaml | envsubst | tee compiled_secret
sem create -f compiled_secret
rm -rf compiled_secret
