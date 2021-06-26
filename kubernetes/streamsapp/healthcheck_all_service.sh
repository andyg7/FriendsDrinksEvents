#!/usr/bin/env bash

set -eu

kubectl exec -it friendsdrinks-request-service-0 -- curl localhost:8080/v1/health
echo '\n'
kubectl exec -it friendsdrinks-writer-service-0 -- curl localhost:8080/v1/health
echo '\n'
kubectl exec -it friendsdrinks-meetup-writer-service-0 -- curl localhost:8080/v1/health
echo '\n'
kubectl exec -it friendsdrinks-membership-writer-service-0 -- curl localhost:8080/v1/health
echo '\n'
kubectl exec -it friendsdrinks-membership-request-service-0 -- curl localhost:8080/v1/health
echo '\n'
kubectl exec -it user-service-0 -- curl localhost:8080/v1/health
echo '\n'
