import pandas as pd
from faker import Faker
import random


def generate_data(num_rows):
    fake = Faker()

    # Generate random data for each column
    data = {
        'name': [fake.name() for _ in range(num_rows)],
        'age': [random.randint(18, 70) for _ in range(num_rows)],
        'zipcode': [fake.zipcode() for _ in range(num_rows)],
        'state': [fake.state() for _ in range(num_rows)],
        'job': [fake.job() for _ in range(num_rows)],
    }

    return pd.DataFrame(data)


def save_to_parquet(df, filename):
    df.to_parquet(filename, index=False)


if __name__ == "__main__":
    df = generate_data(10000)  # Generate 1000 rows of data
    save_to_parquet(df,
                    'random_data.parquet')  # Save the data to a Parquet file
