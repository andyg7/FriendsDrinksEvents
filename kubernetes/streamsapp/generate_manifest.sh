#!/usr/bin/env bash

set -eu

export manifest=$1
export img=$2
export apply_tmp=$3

cat $manifest | envsubst | tee $apply_tmp
cat $apply_tmp