#!/usr/bin/bash
python3 latency_test.py --ms localhost:8080 --op lb --iter 10 --redis-port 6379 --num_cs $1 --path . --num_client $1 --size 30
