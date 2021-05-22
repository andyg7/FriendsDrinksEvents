#!/bin/zsh

set -eu

region=$1

aws iam create-user --user-name KubernetesManifestDeployerUser
aws iam attach-user-policy --user-name KubernetesManifestDeployerUser --policy-arn arn:aws:iam::aws:policy/AWSCloudFormationReadOnlyAccess
new_access_key=$(aws iam create-access-key --user-name $user | jq -r '.AccessKey')
echo "New access key $new_access_key"

aws secretsmanager --region $region create-secret --name KubernetesManifestDeployerUserCredentials --secret-string "$new_access_key"

export AWS_ACCESS_KEY_ID=$(echo $new_access_key | jq -r '.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo $new_access_key | jq -r '.SecretAccessKey')

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
cat envautomation/awsiamusersecret.yaml | envsubst | tee $compiled_secret
sem create -f $compiled_secret
rm -rf $compiled_secret
