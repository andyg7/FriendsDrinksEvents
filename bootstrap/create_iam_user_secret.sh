#!/bin/zsh

set -eu

region=$1

aws iam create-user --user-name KubernetesManifestDeployerUser
aws iam attach-user-policy --user-name KubernetesManifestDeployerUser --policy-arn arn:aws:iam::aws:policy/AWSCloudFormationReadOnlyAccess
access_key=$(mktemp)
echo "$access_key"
aws iam create-access-key --user-name KubernetesManifestDeployerUser | jq -r '.AccessKey' > "$access_key"

cat $access_key

export AWS_ACCESS_KEY_ID=$(jq -r '.AccessKeyId' "$access_key")
export AWS_SECRET_ACCESS_KEY=$(jq -r '.SecretAccessKey' "$access_key")

aws secretsmanager --region $region create-secret --secret-name KubernetesManifestDeployerUserCredentials --secret-string "{\"AWS_ACCESS_KEY_ID\": \"$AWS_ACCESS_KEY_ID\", \"AWS_SECRET_ACCESS_KEY\":\"$AWS_SECRET_ACCESS_KEY\"}"

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
