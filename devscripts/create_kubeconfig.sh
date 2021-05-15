#!/usr/bin/env bash

set -eu

export AWS_ACCESS_KEY_ID=$1
export AWS_SECRET_ACCESS_KEY=$2
export AWS_ROLE_ARN=$3
export AWS_DEFAULT_REGION=$4

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

export STS_CREDENTIALS=$(aws sts assume-role --role-arn $AWS_ROLE_ARN --role-session-name KubeconfigSessionName | jq -r '.Credentials') >/dev/null 2>&1
export AWS_ACCESS_KEY_ID_STS=$(echo $STS_CREDENTIALS | jq -r '.AccessKeyId') >/dev/null 2>&1
export AWS_SECRET_ACCESS_KEY_STS=$(echo $STS_CREDENTIALS | jq -r '.SecretAccessKey') >/dev/null 2>&1
export AWS_SESSION_TOKEN_STS=$(echo $STS_CREDENTIALS | jq -r '.SessionToken') >/dev/null 2>&1
export AWS_ACCESS_KEY_ID=$(echo $AWS_ACCESS_KEY_ID_STS) >/dev/null 2>&1
export AWS_SECRET_ACCESS_KEY=$(echo $AWS_SECRET_ACCESS_KEY_STS) >/dev/null 2>&1
export AWS_SESSION_TOKEN=$(echo $AWS_SESSION_TOKEN_STS) >/dev/null 2>&1

aws eks update-kubeconfig --name EKSCluster --kubeconfig awskubeconfig
cat awskubeconfig
kubectl --kubeconfig awskubeconfig cluster-info
