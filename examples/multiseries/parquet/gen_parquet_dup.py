import pandas as pd
import numpy as np
import argparse
import random
import concurrent.futures


def parse_args():
    parser = argparse.ArgumentParser(
        description='Generate a Parquet file containing duplicate entries.')
    # n for the number of rows to generate
    parser.add_argument('-n', type=int, dest='num_rows', default=1_000,
                        help='Number of rows to generate')
    # o for the output file prefix
    parser.add_argument('-o', type=str, dest='output_file_prefix',
                        default='./data',
                        help='Output file prefix')
    # r for the ratio of duplicates
    parser.add_argument('-r', type=float, dest='ratio', default=0.1,
                        help='Ratio of duplicates [0.0 to 1.0)')

    # m for min string length
    parser.add_argument('-m', type=int, dest='min_string_length', default=1,
                        help='Minimum string length')

    # M for max string length
    parser.add_argument('-M', type=int, dest='max_string_length', default=20,
                        help='Maximum string length')

    # b for batch size
    parser.add_argument('-b', type=int, dest='batch_size', default=1_000_000,
                        help='Batch size for parallel generation')

    return parser.parse_args()


def gen_random_string(min_length, max_length):
    length = np.random.randint(min_length, max_length)
    return ''.join(np.random.choice(list('abcdefghijklmnopqrstuvwxyz'), length))


def generate_data(num_rows, dup_ratio, min_string_length, max_string_length,
                  out_file_path):
    unique_data = set()
    data = []
    for _ in range(num_rows):
        rnd = np.random.rand()
        if rnd < dup_ratio and len(unique_data) > 0:
            str = np.random.choice(data)
            data.append(str)
        else:
            while True:
                str = gen_random_string(min_string_length, max_string_length)
                if str not in unique_data:
                    unique_data.add(str)
                    data.append(str)
                    break

    # Make sure the data is shuffled
    np.random.shuffle(data)

    assert len(data) == num_rows

    df = pd.DataFrame(data)
    df.to_parquet(out_file_path, index=False)


def generate_data_parallel(num_rows, dup_ratio, min_string_length,
                           max_string_length, output_file_prefix,
                           batch_size):
    batch_size = min(num_rows, batch_size)
    num_batches = (num_rows + batch_size - 1) // batch_size

    with concurrent.futures.ProcessPoolExecutor(max_workers=128) as executor:
        batch_sizes = [batch_size] * num_batches
        file_names = [f'{output_file_prefix}-{i}.parquet' for i in
                      range(num_batches)]
        for i in range(num_batches):
            executor.submit(generate_data, batch_sizes[i], dup_ratio,
                            min_string_length, max_string_length, file_names[i])


if __name__ == "__main__":
    args = parse_args()
    generate_data_parallel(args.num_rows, args.ratio, args.min_string_length,
                           args.max_string_length, args.output_file_prefix,
                           args.batch_size)
    print(f"Data saved to {args.output_file_prefix}-*.parquet")
