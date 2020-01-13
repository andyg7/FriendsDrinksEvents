#!/usr/bin/env bash

PORT=$1
EMAIL=$2

curl -X GET http://localhost:"${PORT}"/v1/emails/"${EMAIL}"
