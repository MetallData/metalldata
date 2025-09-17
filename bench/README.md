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
