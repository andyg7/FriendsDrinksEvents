#!/usr/bin/env bash

PORT=$1
REQUEST_ID=$2

curl -X GET http://localhost:"${PORT}"/v1/requests/"${REQUEST_ID}"
