#!/usr/bin/env bash

set -eu

img=$1
action=$2

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_api_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_api_service_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_invitation_writer_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_meetup_writer_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_membership_request_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_membership_writer_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_request_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_writer_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/user_service.yml $img $apply_tmp
kubectl $action -f $apply_tmp
rm -rf $apply_tmp

echo 'Success!'
