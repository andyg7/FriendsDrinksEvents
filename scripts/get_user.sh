#!/usr/bin/env bash

PORT=$1

curl -d '{"requestId":"2f66deeb-0928-4cbb-93c6-dcd00298579"}' -H "Content-Type: application/json" -X GET http://localhost:"${PORT}"/v1/user
