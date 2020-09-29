#!/usr/bin/env bash

PORT=32778
USER_ID=$2

curl -X POST http://localhost:"${PORT}"/v1/users/"${USER_ID}"