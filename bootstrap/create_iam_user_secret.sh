#!/bin/zsh

set -e

aws iam create-user --user-name KubernetesManifestDeployerUserTest
aws iam put-user-policy --user-name KubernetesManifestDeployerUserTest --policy-name AWSCloudFormationReadOnlyAccess
access_key=$(mktemp)
echo "$access_key"
aws iam create-access-key --user-name KubernetesManifestDeployerUserTest | jq -r '.AccessKey' > "$access_key"

cat $access_key

export AWS_ACCESS_KEY_ID=$(jq -r '.AccessKeyId' "$access_key")
export AWS_SECRET_ACCESS_KEY=$(jq -r '.SecretAccessKey' "$access_key")

rm -rf $access_key

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
cat bootstrap/awsiamusersecret.yaml | envsubst | tee $compiled_secret
sem create -f $compiled_secret
rm -rf $compiled_secret
