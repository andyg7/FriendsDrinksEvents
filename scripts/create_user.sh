#!/usr/bin/env bash

PORT=$1
EMAIL=$2

curl -d "{\"email\":\"${EMAIL}\"}" -H "Content-Type: application/json" -X POST http://localhost:"${PORT}"/v1/users