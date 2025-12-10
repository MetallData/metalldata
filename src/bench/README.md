For `remove_if` and `remove_if2` (and anything that uses the `jsonlogic/apply_jl` function), an example jsonlogic expression might be

```json
{"rule":{">":[{"var":"age"},60]}}
```

This is an expression that translates to "age > 60". For `remove_if` and `remove_if2`, it can be specified in a file (using the `--jl_file` option) or passed in via `STDIN`.

Commands for mframe_bench:

```sh
./mframe_bench gen-multiseries --metall_path data/m4 -n 10000 -s name:name -s age:int_percentage -s score:percentage -s uuid:uuid4

./mframe_bench filter_to_parquet --metall_path=data/m4 --parquet_file=pqage --jl_file=age_filter.json
```


For a graph,
```
for i in $(seq 5); do ./mframe_bench gen-multiseries --metall_path data/graph$i -n 1000 -s from:two_char_string -s to:two_char_string -s conn_id:uuid4 -s age:int_percentage -s score:percentage; done

for i in $(seq 5); do ./mframe_bench filter_to_parquet --metall_path=data/graph$i --parquet_file=pqgraph$i --jl_file=filter_all.json; done
```
