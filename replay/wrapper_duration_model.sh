#!/bin/bash
model_version=$1
message_file=$2
rule_file="./duration_rule_"${model_version}".txt"

cat $2 | python duration_model_warning.py --rulefile $rule_file --replay --pub tcp://*:30115


