#!/bin/bash
PYTHON_PATH="..."

# Device 1 id
DEV1="..."
# Device 2 id
DEV2="..."
# Stats to monitor (space-separated)
# E.g. signal, downlinkCapacity, uplinkCapacity, etc
STATS="signal"
# API endpoint
ENDPOINT=".../nms/api/v2.1"
# UNMS API key
API_KEY="..."
# Path to the sqlite db file
DB=".../data.db"

$PYTHON_PATH ./src/signal_logger.py -k $API_KEY -e $ENDPOINT -d $DEV1 $DEV2 -s $STATS -db $DB