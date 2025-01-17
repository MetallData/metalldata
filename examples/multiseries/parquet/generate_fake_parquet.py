import argparse
import pandas as pd
from faker import Faker
import random
import concurrent.futures


def parse_args():
    parser = argparse.ArgumentParser(description='Generate a Parquet file.')
    # n for the number of rows to generate
    parser.add_argument('-n', type=int, dest='num_rows', default=10000,
                        help='Number of rows to generate')
    # o for the output file prefix
    parser.add_argument('-o', type=str, dest='output_file_prefix',
                        default='./data',
                        help='Output file prefix')

    return parser.parse_args()


def generate_batch_data(num_rows, out_file):
    fake = Faker()

    # Generate random data for each column
    data = {
        'name': [fake.name() for _ in range(num_rows)],
        'age': [random.randint(18, 70) for _ in range(num_rows)],
        'zipcode': [fake.zipcode() for _ in range(num_rows)],
        'state': [fake.state() for _ in range(num_rows)]
    }

    df = pd.DataFrame(data)
    save_to_parquet(df, out_file)


def generate_data(num_rows, batch_size, output_file_prefix):
    batch_size = min(num_rows, batch_size)
    num_batches = (num_rows + batch_size - 1) // batch_size

    with concurrent.futures.ProcessPoolExecutor(max_workers=128) as executor:
        batch_sizes = [batch_size] * num_batches
        file_names = [f'{output_file_prefix}-{i}.parquet' for i in
                      range(num_batches)]
        for i in range(num_batches):
            executor.submit(generate_batch_data, batch_sizes[i], file_names[i])


def save_to_parquet(df, filename):
    df.to_parquet(filename, index=False)


if __name__ == "__main__":
    args = parse_args()
    generate_data(args.num_rows, 10_000, args.output_file_prefix)
    print(f"Data saved to {args.output_file_prefix}-*.parquet")
