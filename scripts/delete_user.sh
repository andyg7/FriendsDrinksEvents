#!/usr/bin/env bash

PORT=32778
USER_ID=$1
curl -d "{\"eventType\": \"DELETED\"}" -H "Content-Type: application/json" -X POST http://localhost:"${PORT}"/v1/users/"${USER_ID}"
