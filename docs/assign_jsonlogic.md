# `assign` with a jsonlogic expression (`assign_jsonlogic`): how the code works

Python's `assign` creates a new node or edge series and writes a value to every row that matches an optional `where` clause. The value can be a **constant**, which is the original behavior, or a **jsonlogic expression**. An expression is evaluated separately on each row, and the result is stored in that row:

```python
mg.assign("edge.flag", True)                                                       # constant
mg.assign("edge.total", {"+": [{"var": "edge.graphnum"}, {"var": "edge.randint"}]})  # raw jsonlogic
mg.assign("edge.label", {"cat": [{"var": "edge.color"}, "-", {"var": "edge.name"}]},
          where=mg.edge.graphnum == 3)
mg.assign("edge.big", mg.edge.randint > 50)            # clippy expression -> bool series
```

The main difficulty is that **the output type is not known in advance**. A jsonlogic result can be bool, int64, double, string, null or an array, and the type can change from row to row. For example, `+` gives int64 for two ints and double if either input is a double. On top of that, each MPI rank sees only its own rows, so every rank has to agree on one type before any of them creates the series.

## Files

| File | Role |
|---|---|
| [include/metalldata/metall_graph.hpp](../include/metalldata/metall_graph.hpp#L225-L235) | Declarations of `assign_value` (renamed from `assign`) and the new `assign_jsonlogic` |
| [src/libmetalldata/metall_graph_jl.hpp](../src/libmetalldata/metall_graph_jl.hpp) | Private helper that compiles a jsonlogic rule into a callable that returns a value |
| [src/libmetalldata/metall_graph_assign_jsonlogic.cpp](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp) | The implementation |
| [src/clippy/MetallGraph/assign.cpp](../src/clippy/MetallGraph/assign.cpp) | Clippy `assign` entry point. Calls `assign_value` or `assign_jsonlogic` depending on the type of `value` |
| [test/clippy/tests/test_mg_assign_jsonlogic.py](../test/clippy/tests/test_mg_assign_jsonlogic.py) | pytest coverage: results, errors, warnings |
| `CMakeLists.txt` in `src/libmetalldata` | Registers the new source file |
| [metall_graph_ingest.cpp](../src/libmetalldata/metall_graph_ingest.cpp#L272-L274), [test_assign.cpp](../test/metall_graph/test_assign.cpp#L88) | Call sites updated for the rename (`test_assign.cpp` is disabled in CMake) |

`metall_graph_where.cpp` now defines the shared `compile_jl_expr` (§2), and its `priv_compile_jl_rule` is a thin wrapper around it. The where clause behaves exactly as before. The only change to the constant path in `metall_graph_assign.cpp` is the rename.

## 1. Public API

```cpp
result<> assign_value(series_name name, const series_types& val,   const where_clause& where); // was assign()
result<> assign_jsonlogic(series_name name, const bjsn::value& jl_rule, const where_clause& where);
```

**Why two named methods rather than overloads?** Only the Python side is dynamically typed. A C++ caller always knows at compile time whether it has a constant or a rule, so the choice between them happens once, in the clippy executable (§4). Python keeps a single `assign`. Overloading in C++ was tried first and is fragile: `bjsn::value` converts implicitly from ints and strings, and `bjsn::object` has a non-explicit `object(std::size_t min_capacity)` constructor, so every `bool`/`int`/`double` converts to it. Either choice made existing calls like `assign(name, v, {})` ambiguous.

The rule itself:
- It is a bare jsonlogic rule such as `{"+": [{"var": "edge.a"}, 1]}`. Variables use qualified series names, just as in a where clause.
- It may only reference series in the **same table** (node or edge) as `name`.
- `where` can be any `where_clause`, a node clause or an edge clause, whatever the target's table.

`assign_jsonlogic` returns `result<>`. **All error checks happen before the series is created, so a returned error (`std::unexpected`) never leaves a series behind.** Once the series exists, nothing can return an error: per-row problems, including exceptions thrown by jsonlogic, are skipped and reported as **warnings** with counts totaled across all ranks. See §3.8 for the one exception (failures outside jsonlogic, such as running out of storage).

## 2. Compiling the expression: `metall_graph_jl.hpp`

[`compile_jl_expr`](../src/libmetalldata/metall_graph_jl.hpp#L39) turns a `boost::json` rule into a [`compiled_jl_expr`](../src/libmetalldata/metall_graph_jl.hpp#L30) with three members:

- `fn`: takes `std::vector<series_types>` (the row values, in the same order as `vars`) and returns the raw `jsonlogic::value_variant`. `string_view` inputs are wrapped as `managed_string_view` with `no_lifetime_management`, so row strings are passed to jsonlogic without being copied.
- `vars`: the variable names the rule references, such as `"edge.randint"`.
- `has_computed_vars`: true if the rule builds variable names at runtime. That isn't supported, because the column list has to be resolved before evaluation starts.

This is the **single** jsonlogic compile path, and both jsonlogic users share it:

- `where_clause`: [`priv_compile_jl_rule`](../src/libmetalldata/metall_graph_where.cpp#L54) wraps it, returning `truthy(fn(row))`.
- `assign_jsonlogic`: calls it directly and converts `fn(row)` with `to_owned` (§3.2).

**Where it lives.** The header only declares `compile_jl_expr`. It is defined in [`metall_graph_where.cpp`](../src/libmetalldata/metall_graph_where.cpp#L13) because of how jsonlogic is packaged:

> `<jsonlogic/src.hpp>`, which `metall_jl.hpp` pulls in, contains **non-inline definitions**, so it can be included in only one source file. If a second file includes it, the link fails with multiple-definition errors. `metall_graph_where.cpp` is that file: it also needs `metall_jl.hpp` for `jl::parseFile`/`jl::parseStream`. Anything else that needs jsonlogic includes only `metall_graph_jl.hpp`, which pulls in just `<jsonlogic/logic.hpp>` (declarations).

## 3. Implementation: `metall_graph_assign_jsonlogic.cpp`

### 3.1 Helper types (anonymous namespace)

- **[`value_kind`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L36)**: `none < boolean < integer < floating`, plus `string`. The numeric kinds are ordered from narrowest to widest, so "widest numeric kind" is just `max`, which the cross-rank agreement in §3.6 relies on.
- **[`warning_idx`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L61) / `warning_msgs`**: a fixed list of reasons a row can be skipped. The counts go in a `std::array` rather than directly into `result<>`, so they can be summed across ranks with one `ygm::sum` per entry (§3.7).
- **`owned_value`**: an alias for `metall_graph::data_types` (`monostate, bool, int64_t, double, std::string`). It is the owned, row-independent form of a result.

### 3.2 Converting a jsonlogic result: `to_owned`

[`to_owned`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L90) maps each `value_variant` alternative:

| jsonlogic result | becomes | warning |
|---|---|---|
| `bool`, `int64_t`, `double` | same | none |
| `uint64_t` | `int64_t` if ≤ `INT64_MAX`, else unset | "does not fit in int64" |
| `managed_string_view` | **copied** into `std::string` | none |
| `array_value const*` | unset | "returned an array" |
| `nullptr_t` / `monostate` | unset | "returned null" |

The string copy is required. A string that jsonlogic builds during evaluation (for example with `cat`) is owned by the rule, and the view to it is only valid until the next evaluation.

### 3.3 Storing with widening: `kind_of`, `store_as<T>`

- [`kind_of`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L117) returns the `value_kind` of an `owned_value`.
- [`store_as<T>`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L139) writes a value into a series of type `T` through a setter callback. It accepts:
  - an exact match (`std::string` → `std::string_view` counts as exact for string series)
  - `bool → int64_t`
  - `bool / int64_t → double`

  Anything else returns `false`: narrowing `double → int64_t`, or mixing strings and numbers. The caller then counts a "could not be stored" warning and leaves the cell unset. Nothing is silently truncated.

### 3.4 Validation (errors, before the series is created)

[`assign_jsonlogic`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L168) first rejects each of these cases. Every check depends only on the rule and on the series list, which is the same on every rank, so all ranks fail together and none is left waiting in a collective.

| Check | Error message contains |
|---|---|
| target is not `node.`/`edge.` | `unknown series name` |
| target already exists | `already exists` |
| jsonlogic can't parse the rule ([line 183](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L183)) | `invalid jsonlogic expression` |
| rule builds variable names at runtime | `computed variable names` |
| a variable's table differs from the target's ([line 195](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L195)) | `is not a node series` / `is not an edge series` |
| a variable names a missing series | `series <name> not found` |

After these checks, `store` points to `m_pnodes` or `m_pedges`, and `var_idxs` holds the column index of each variable.

### 3.5 Per-row evaluation: `eval` and `for_all_matching`

- [`eval(row_index)`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L220) reads each variable's value with `store->get_dynamic`, runs `expr.fn`, and passes the result through `to_owned`. If any input cell is None (`is_none`), it counts "an input variable is missing" and returns unset without evaluating. This is the same rule the where clause uses. The `row` vector is reused across calls to avoid a heap allocation per row.
- **Evaluation exceptions are caught per row** ([line 232](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L232)). jsonlogic throws on some inputs, for example `"red" + 1`. The row loops are collective, so an exception on one rank would leave the other ranks waiting in a barrier forever. Early versions of this code did exactly that and hung. Now the row is skipped and counted as "expression raised an error".
- [`for_all_matching(fn)`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L241) calls `priv_for_all_nodes(…, where)` or `priv_for_all_edges(…, where)` with local row indices. This reuses all the existing where-clause handling, including node clauses on edge targets and edge clauses on node targets. These helpers are **collective**, so every rank has to call them the same number of times.

### 3.6 Type inference: the two-pass design

**Pass 1, probe ([line 251](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L251)).** Each rank walks its matching rows and evaluates only until it gets the first non-null result. `local_kind` is that result's kind. Once it's set, the remaining rows are still visited, because the helper is collective, but not evaluated, so this pass costs little. Warnings from this pass are discarded (`local_warnings.fill(0)`) because pass 2 re-evaluates the same rows and would otherwise count them twice.

**Agreement across ranks ([line 261](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L261)).** Two collectives:

- `any_string = ygm::logical_or(local_kind == string)`
- `max_numeric = ygm::max(local numeric kind, or 0 for string/none)`

Then:

| outcome | result |
|---|---|
| strings on some ranks and numbers on others | **error**: "produces both string and … values" |
| no rank produced a value | **error**: "produced no values; … was not created" |
| otherwise | string, or the widest numeric kind across ranks |

For example, if rank 0's first value is an int and rank 1's is a double, the series becomes double. `none` acts as the identity: a rank with no matching rows doesn't affect the outcome. "Produced no values" covers several cases:
- `where` matches no rows, including a `where` that names a missing series, which the existing where helpers treat as matching nothing;
- every row's input is missing;
- every row returns null or an array;
- every row raises an error.

**Pass 2, write ([line 278](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L278)).** The generic lambda `write_all<T>` creates the series with `store->add_series<T>` and walks the matching rows again. For each row it calls `eval` and then `store_as<T>` to write the value. The [`switch (kind)`](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L294) turns the runtime `value_kind` into the compile-time `T`.

**A caveat of "first value wins."** A rank picks its type from its *first* value only. If a later row on the same rank produces a wider type (a double after the type was settled as int64), that row can't be stored and is counted as a "could not be stored in an int64 series" warning. For the same reason, an expression that returns strings for some rows and numbers for others either errors (ranks disagree) or stores one kind and warns about the rest (ranks agree). Which one happens depends on how rows are partitioned. To avoid all this, write expressions whose result type is stable across rows (for example, multiply by `1.0` to force a double).

Why two passes instead of buffering every result from pass 1? Pass 1 stops evaluating early, so it's nearly free, and pass 2 needs no memory proportional to the number of rows. The cost is that the where-clause filter runs twice, and for node/edge cross clauses that includes communication.

### 3.7 Warnings

Each rank counts warnings in `local_warnings`. At the end ([line 311](../src/libmetalldata/metall_graph_assign_jsonlogic.cpp#L311)), each count is summed with `ygm::sum` and nonzero totals are added with `add_warnings(n, msg)`. Every rank therefore returns the same **global** counts, and clippy prints them once as `<message> : <count>`.

| Warning | Cause |
|---|---|
| `row skipped: an input variable is missing` | a referenced cell is None |
| `row skipped: expression raised an error` | jsonlogic threw on this row |
| `row skipped: expression returned null` / `…an array` | result can't be stored |
| `row skipped: unsigned result does not fit in int64` | `uint64` > `INT64_MAX` |
| `row skipped: result could not be stored in a <type> series` | row's type doesn't widen to the inferred type |

### 3.8 Error guarantee

Every `return std::unexpected(...)` in `assign_jsonlogic` comes before `add_series` in pass 2 (validation in §3.4, type agreement in §3.6). After the series is created there are no error returns, so **a returned error never leaves a series behind.**

Failures inside jsonlogic can't break this, because each one is caught before the series exists or turned into a per-row warning. Regex is a good example:

| Case | Handling |
|---|---|
| Invalid regex pattern written in the rule | Compiled up front by `create_logic`; the `regex_error` becomes `invalid jsonlogic expression`, before the series exists |
| Invalid regex pattern taken from a series value | Compiled per row; the exception is caught in `eval` → "expression raised an error" warning |
| Out-of-range index into a `regex_strings` result | jsonlogic's `elem_at` catches `std::out_of_range` and returns `null` → "expression returned null" warning |

**Not covered:** exceptions thrown outside jsonlogic during pass 2, such as Metall running out of space in `add_series`/`set`/`add_string`, `std::bad_alloc`, or a failure inside the where-clause helpers. These escape with the series already created and partly filled. Under MPI, the other ranks would also probably hang in the next collective. They're treated as fatal: recovering cleanly would need all ranks to agree that something failed mid-loop. Constant `assign_value` has the same exposure.

## 4. Clippy entry point: `src/clippy/MetallGraph/assign.cpp`

The `value` parameter is now a `boost::json::value` instead of `series_types`. Dispatch ([line 52](../src/clippy/MetallGraph/assign.cpp#L52)):

- **Object** → `assign_jsonlogic`. It accepts two forms:
  - a clippy expression (`mg.edge.randint > 50`), which Python serializes as `{"expression_type": "jsonlogic", "rule": …}`. `obj["rule"]` is used.
  - a raw jsonlogic dict (`{"+": [...]}`), which is used as-is. This form is needed because the Python `jsonlogic.Operand` only overloads comparison operators, so arithmetic, `cat`, `if` and similar have to be written as dicts.
- **Anything else** → `value_to<series_types>` → `assign_value`, exactly as before. A string constant is interned into the Metall string store when it is written.

Behavior changes in this wrapper, which apply to constant assignments too:

- **Errors now exit non-zero.** The old wrapper ignored the C++ method's return value, so failures such as "series already exists" went unreported. It now prints the error and returns -1, which raises `NonZeroReturnCodeError` in Python. This exposed a latent bug in `test_mg_assign.py`: it re-assigned `node.gnum`, which the fixture had already created. The test now uses `node.gnum_assigned`.
- **Warnings are printed.**
- **Uncaught exceptions exit non-zero** (the `catch` blocks at [line 76](../src/clippy/MetallGraph/assign.cpp#L76)). Previously they fell off the end of `main` and exited 0.

## 5. Tests: `test/clippy/tests/test_mg_assign_jsonlogic.py`

Helpers: `warning_count(out, msg)` parses `msg : N` from the captured clippy output, and `assert_not_created(mg, name)` checks that a failed call left no series behind.

| Group | Tests |
|---|---|
| **Results and type inference** | int+int → int64; double column + int → double; int × 0.5 → double; `cat` → string, with missing inputs left unset; clippy expression → bool; `where` limits which rows are written; node-table expression; constants still work (string with `where`, int) |
| **Errors** (specific message, no series created) | target exists; bad target name (`nodot`, `foo.bar`); cross-table variable (both directions); unknown variable; invalid operator; computed variable name; `where` matches nothing; `where` names a missing series; every row raises (the former deadlock); every row returns an array |
| **Warnings** (exact counts checked against the data) | missing input; null result, where the non-null rows still get written; some rows raise, counting raised and missing rows separately; mixed string/number, which accepts either the cross-rank error or the per-row warnings depending on partitioning |

Run the tests (`CLIPPY_CMD_PREFIX=mpirun` runs every call on multiple ranks):

```bash
cd test/clippy
CLIPPY_CMD_PREFIX=mpirun CLIPPY_BACKEND_PATH=$PWD/../../build/src/clippy \
  DATA_DIR=../../data/metall_graph pytest tests/test_mg_assign_jsonlogic.py
```
