#!/bin/bash

JOB=$1
SRC=$2
DST=$3

SLEEP=$((RANDOM % 46 + 5))
sleep $SLEEP

RAND=$((RANDOM % 100))
if [ "$RAND" -lt 5 ]; then
	echo oh nose fail
	exit 1
else
	echo yay
	exit 0
fi
