"""
Converts JSON files to Parquet files using Panda.
"""

import argparse
import pandas as pd
import concurrent.futures
import os
import json

def parse_args():
    parser = argparse.ArgumentParser(description='Convert NDJSON files to Parquet files.')
    # i for the input file
    parser.add_argument('-i', type=str, dest='input_file', required=True,
                        help='Input file')
    # o for the output file
    parser.add_argument('-o', type=str, dest='output_file',
                        help='Output file')
    return parser.parse_args()

def convert_json_to_parquet(input_file, output_file):
    with open(input_file) as f:
        data = [json.loads(line) for line in f]
    df = pd.DataFrame(data)
    df.to_parquet(output_file)

if __name__ == "__main__":
    args = parse_args()
    convert_json_to_parquet(args.input_file, args.output_file)
