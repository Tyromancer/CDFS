#!/usr/bin/env bash
python3 latency_test.py --ms localhost:8080 --op $1 --iter 10 --redis-port 6379 --num_cs 3 --path . --num_client 3 --size $2