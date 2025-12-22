#!/bin/sh

for i in csv/*.csv; do
    python3 csv2pq/csv2pq.py $i
done