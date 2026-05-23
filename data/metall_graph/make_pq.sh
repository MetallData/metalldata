#!/bin/sh

for i in "$@"; do
    python3 csv2pq/csv2pq.py $i
done
