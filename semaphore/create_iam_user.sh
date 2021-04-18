#!/bin/zsh

set -e

export IAM_ROLE=$1

if [ -z "$IAM_ROLE" ]
then
      echo "\$IAM_ROLE is not set"
      exit 1
fi


compiled_secret=$(mktemp)
cat semaphore/awsiamrole.yaml | envsubst | tee compiled_secret
sem create -f compiled_secret
rm -rf compiled_secret
