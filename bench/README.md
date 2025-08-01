For `remove_if` and `remove_if2` (and anything that uses the `jsonlogic/apply_jl` function), an example jsonlogic expression might be

```json
{"rule":{">":[{"var":"age"},60]}}
```

This is an expression that translates to "age > 60". For `remove_if` and `remove_if2`, it can be specified in a file (using the `--jl_file` option) or passed in via `STDIN`.

