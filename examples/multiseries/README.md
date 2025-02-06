# Multi-series Example

## Bun Demo

All executable take `-h` option to show help messages.

```shell
cd metalldata/build

python3 ../examples/multiseries/parquet/gen_parquet_fake.py -o fake-data/data -n 10000

mpirun -n 2 ./examples/multiseries/ingest_parquet -i ./fake-data/

# find max (name, string type)
mpirun -n 2 ./examples/multiseries/find_max -d metall_data/ -s name -t s

# find max (age, double type)
# Note: age data is stored as double in multi-series as it is a float type in the parquet file to hold 'nan' values
mpirun -n 2 ./examples/multiseries/find_max -d metall_data/ -s age -t d
```