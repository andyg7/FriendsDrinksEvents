#!/usr/bin/env bash

PORT=$1
USER_ID=$2

if [ -n "$USER_ID" ]; then
  curl -X GET http://localhost:"${PORT}"/v1/friendsdrinks/"${USER_ID}"
else
  curl -X GET http://localhost:"${PORT}"/v1/friendsdrinks
fi
