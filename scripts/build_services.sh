#!/usr/bin/env bash

docker build -t andyg001/friendsdrinksbackend:$(date -u +"%Y%m%dT%H%M%SZ") .
