#!/usr/bin/env bash

PORT=$1
ADMIN_USER_ID=$2
USER_ID=$3
NAME=$4

curl -d "{\"adminUserId\":\"${ADMIN_USER_ID}\", \"name\":\"${NAME}\", \"scheduleType\": \"OnDemand\", \"userIds\": [\"${USER_ID}\"] }" \
-H "Content-Type: application/json"  \
-X POST http://localhost:"${PORT}"/v1/friendsdrinks
