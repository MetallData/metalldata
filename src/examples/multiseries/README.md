# Multi-series Example

## Run Demo

All executable take `-h` option to show help messages.

```shell
cd metalldata/build

python3 ../examples/multiseries/parquet/gen_parquet_fake.py -o fake-data/data -n 10000

mpirun -n 2 ./examples/multiseries/ingest_parquet -i ./fake-data/

# find max (name and age)
mpirun -n 2 ./examples/multiseries/find_max -d metall_data/ -s name,age
```