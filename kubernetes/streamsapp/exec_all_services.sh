#!/usr/bin/env bash

set -eu

action=$1

kubectl $action -f kubernetes/streamsapp/10create_kafka_topics_job.yml
kubectl $action -f kubernetes/streamsapp/20streams_config.yml

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_api_service.yml andyg001/friendsdrinksbackend:latest $apply_tmp
kubectl $action -f $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_api_service_service.yml andyg001/friendsdrinksbackend:latest $apply_tmp
kubectl $action -f $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_invitation_writer_service.yml andyg001/friendsdrinksbackend:latest $apply_tmp
kubectl $action -f $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_meetup_writer_service.yml andyg001/friendsdrinksbackend:latest $apply_tmp
kubectl $action -f $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_membership_request_service.yml andyg001/friendsdrinksbackend:latest $apply_tmp
kubectl $action -f $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_membership_writer_service.yml andyg001/friendsdrinksbackend:latest $apply_tmp
kubectl $action -f $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_request_service.yml andyg001/friendsdrinksbackend:latest $apply_tmp
kubectl $action -f $apply_tmp

apply_tmp=$(mktemp)
./kubernetes/streamsapp/generate_manifest.sh kubernetes/streamsapp/friendsdrinks_writer_service.yml andyg001/friendsdrinksbackend:latest $apply_tmp
kubectl $action -f $apply_tmp

rm -rf $apply_tmp

echo 'Success!'
