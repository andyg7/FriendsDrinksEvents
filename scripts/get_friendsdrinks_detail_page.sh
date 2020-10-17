#!/usr/bin/env bash

PORT=32778
FRIENDSDRINKS_ID=$1
curl -X GET http://localhost:"${PORT}"/v1/friendsdrinksdetailpage/"${FRIENDSDRINKS_ID}"
