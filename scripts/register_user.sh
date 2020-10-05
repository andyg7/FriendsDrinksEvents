#!/usr/bin/env bash

PORT=32778
curl -d "{\"eventType\": \"SIGNED_UP\", \"userSignedUp\":{\"userId\":\"andrewgrant243@gmail.com\", \"firstName\":\"Andrew\", \"lastName\": \"Grant\"}}" -H "Content-Type: application/json" -X POST http://localhost:"${PORT}"/v1/users/andrewgrant243@gmail.com