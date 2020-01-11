#!/usr/bin/env bash


PORT=$1
USER_ID=$2

curl -X DELETE http://localhost:"${PORT}"/v1/users/"${USER_ID}"
