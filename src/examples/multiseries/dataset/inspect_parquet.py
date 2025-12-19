import argparse
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description='Inspect a Parquet file.')
    parser.add_argument('filepath', type=str, help='Path to the Parquet file')

    args = parser.parse_args()

    # Read the Parquet file metadata
    parquet_file = pq.ParquetFile(args.filepath)
    print("Metadata:", parquet_file.metadata)

    # Inspect the schema of the Parquet file
    print("Schema:", parquet_file.schema)

    # Read some rows of the file
    table = parquet_file.read_row_group(0)  # Reading the first row group
    print("Data Preview:")
    print(table.to_pandas())


if __name__ == "__main__":
    main()
