#!/usr/bin/env bash

PORT=$1

curl -X GET http://localhost:"${PORT}"/v1/users/deleteRequests
