# PruneJuice Local Constraint Checking (LCC) for MetallGraph

## Context
PruneJuice (Reza et al., SC18) finds every vertex and edge that takes part in at least one exact match of a small template graph G0 inside a large background graph G. It does this by **pruning**, not by enumerating matches. Recall is always 100%: nothing that is part of a match is ever removed. LCC is the first stage and does most of the pruning (paper Algs. 2–4). The later stages (NLCC: cycle and path constraints, template-driven search) are out of scope here, but the design should leave room for them.

**Paper semantics.** Each vertex v keeps ω(v) ⊆ V0, the template vertices it could still match. It also keeps ε(v), its active neighbors, each tagged with the ω the neighbor last reported. Each round:
1. Every active vertex sends ω(v) to its active neighbors.
2. **Edge elimination:** drop neighbor v_i if it did not report, or if no pair q∈ω(v), q'∈ω(v_i) is a template edge (the paper's φ).
3. **Vertex refinement:** drop q from ω(v) if v's remaining neighbors cannot cover adj(q). The paper checks this as "a minimum number of distinct neighbors per label".
4. A vertex becomes inactive when ω(v) or ε(v) is empty.

Rounds repeat until no vertex or edge changes.

**Decisions made with the user:**
- **Output: a single edge bool series.** No node output is needed. Templates must be connected and have at least one edge, so at the fixpoint every surviving vertex has a surviving edge and every surviving edge has two surviving endpoints. The surviving node set is therefore exactly the endpoints of surviving edges. That is what `where=mg.edge.<out>` already returns for nodes, through the `_ewhere` path in `metall_graph_priv_for_all.ipp`. It is also how an edge-expression subgraph works in `subgraph_plan.md`, so `create_subgraph("m", mg.edge.<out>)` will work for free later.
- **Direction:** undirected, as in the paper. Template edges and background edges are both treated as undirected. Parallel edges collapse into one adjacency entry, and self-loops are ignored.
- **Template:** each template vertex and each template edge carries an optional jsonlogic predicate. A missing predicate matches everything. Each predicate is evaluated with the **existing where-clause semantics**, so it may reference node series or edge series:
  - **Vertex predicate:** goes through `priv_for_all_nodes(fn, pred)`. A node clause tests the node itself. An edge clause matches nodes that touch a matching edge.
  - **Edge predicate:** goes through `priv_for_all_edges(fn, pred)`. An edge clause tests the edge itself. A node clause matches edges whose two endpoints both match.

  Labels are just the special case `mg.node.color == "red"`.

## Design

### Library API (`include/metalldata/metall_graph.hpp`)
```cpp
struct pattern_template {            // new: include/metalldata/impl/metall_graph_pattern.hpp
  std::vector<std::string>  vertex_names;   // template vertex ids ("a","b",...)
  std::vector<where_clause> vertex_preds;   // empty clause = wildcard
  struct edge { size_t u, v; where_clause pred; };
  std::vector<edge>         edges;
  result<> validate() const;  // ≤64 vertices & ≤64 edges, ≥1 edge, connected,
                              // no self loops / duplicate edges, endpoints valid
};

result<lcc_stats> local_constraint_check(const series_name& out_edge_series,
                                         const pattern_template& tmpl,
                                         const where_clause& where);
```
- `lcc_stats` reports the number of rounds and the number of surviving nodes and edges, all global.
- The output name must satisfy `is_edge_series()` and must not exist yet. The check matches `connected_components` in `src/libmetalldata/metall_graph_cc.cpp`.
- Each template has at most 64 vertices and 64 edges, so ω(v) and the per-edge template-edge sets both fit in a `uint64_t` bitmask.

### Implementation (`src/libmetalldata/metall_graph_lcc.cpp`, added to `src/libmetalldata/CMakeLists.txt`)
This follows the BSP form of Alg. 3.
- **State:** a rank-local `boost::unordered_flat_map<local_node_idx_type, lcc_vertex>` on each node's owner rank.
- **Messages:** sent with `m_comm.async(owner(nl), ...)`, using the static-pointer pattern from `metall_graph_cc.cpp`.
- **Table access:** the node and edge tables are reached only through accessors (`has_series`, `priv_add_edge_series`, `pl_set_edge_field`, `pl_get_edge_uv_locators`, `priv_for_all_*`), never through `m_pnodes` / `m_pedges` directly.

```cpp
struct lcc_nbr  { node_locator nbr; uint64_t emask;            // union of template-edge bits over parallel edges
                  std::vector<std::pair<edge_locator,uint64_t>> edges; // per-edge masks, for final output
                  uint64_t recv; };                            // ω(nbr) received this round (0 = none)
struct lcc_vertex { uint64_t omega; std::vector<lcc_nbr> adj; bool active; };
```

1. **Template precompute (local, every rank).** For each template vertex q, compute `tadj[q]` as (template neighbor q', template edge index e) pairs and `tadj_mask[q]`. For each template edge e, record its endpoints.
2. **Candidate init.** For each template vertex q, run one `priv_for_all_nodes(fn, vertex_preds[q])` pass that does `async_visit(make_node_locator(rank, nid), set bit q)`. A wildcard uses the unfiltered `priv_for_all_nodes(fn)`. Do one more pass over the global `where` to mark nodes as in-scope, and AND it in, so out-of-scope nodes get ω = 0.
3. **Edge masks.** For each template edge e, run one `priv_for_all_edges(fn, edges[e].pred)` pass that records bit e for that `local_edge_idx_type` in a rank-local `unordered_flat_map<local_edge_idx_type,uint64_t>`. Edges are stored on their own rank, so no communication is needed.
4. **Adjacency build.** Run `priv_for_all_edges(fn, where)`:
   - Skip u == v and edges whose mask is 0.
   - Otherwise `async_visit(u, add {v, make_edge_locator(rank,eid), mask})`, and the same for v.
   - Then a local pass merges parallel entries per neighbor and sets `active = omega != 0 && !adj.empty()`.
5. **Rounds.** Loop until a global `all_reduce` reports that nothing changed:
   - **Send:** each active vertex v does `async_visit(nbr, recv_visitor, v, omega)` for every adjacency entry. `recv_visitor` sets `recv` on the matching entry, so it is found by locator and messages from removed neighbors are ignored. `barrier()`.
   - **Edge check:** for each entry, compute `phi = recv & allowed(omega(v), emask)`. Here `allowed` is the OR over q∈ω(v) and (q',e)∈tadj[q] with bit e set in emask of bit q'. Drop the entry when `phi == 0`. Otherwise store `recv = phi`.
   - **Vertex check:** for each q∈ω(v), the template neighbors adj(q) need *distinct* background neighbors. This is a small bipartite matching. On the left are the template neighbors q' of q (at most 63). On the right are v's neighbors, grouped by their signature `{q' : q'∈recv and template edge (q,q') ∈ emask}`, with group size as capacity. Run augmenting-path matching on the left side and stop early once all of adj(q) is matched. Drop q when matching fails.
     - This is exact for the one-hop neighborhood. It subsumes both the paper's "every template neighbor's label is present" check and its "minimum distinct same-label neighbors" check.
     - Cost is O(|adj(q)| · #distinct signatures) per q, independent of degree after grouping.
   - **Deactivate:** when ω or adj is empty, set `active = false`; the vertex stops sending. Reset `recv = 0` for the next round, and record whether anything changed.
6. **Output.**
   - **Create the series:** add the edge series `bool` with `priv_add_edge_series<bool>`, then set it to `false` on every in-scope edge with `priv_for_all_edges(fn, where)`. Writing an explicit false lets `!edge.out` work as a where clause.
   - **Mark survivors:** for each active vertex v and adjacency entry (where v < nbr, to avoid duplicates), go through each per-edge `(eloc, mask)`. The edge survives if some q∈ω(v), q'∈phi with template edge (q,q') has bit e in *that edge's own* mask. For each survivor, `m_comm.async(owner(eloc), set true)`, using the lambda pattern from `pasync_set_node_column_by_locator` in `include/metalldata/impl/metall_graph_set_column.ipp`. `barrier()`.

   This per-edge recheck matters for multigraphs. Parallel edges whose own mask fails stay false even when a sibling edge survives.

### Clippy front end (`src/clippy/MetallGraph/local_constraint_check.cpp`, plus a `CMakeLists.txt` entry)
Modeled on `src/clippy/MetallGraph/connected_components.cpp`.
- **Arguments:** `output` (string, default prefix `edge`), `template` (object) and optional `where`.
- **Template JSON:** nested Python expressions arrive as `{"expression_type":"jsonlogic","rule":...}`, as `clippy/backends/serialization.py` shows, so this works directly:
  ```python
  mg.local_constraint_check("m", {"edges": [["a", "b"], ["b", "c"], ["c", "a"]]})  # any triangle
  mg.local_constraint_check("m2", {
      "edges": [["a", "b"], ["b", "c", mg.edge.weight > 5], ["c", "a"]],
      "nodes": {"a": mg.node.color == "red", "b": mg.node.color == "blue"},
  })
  mg.select_edges(where=mg.edge.m);  mg.select_nodes(where=mg.edge.m)
  ```
  Template vertices are the edge endpoints. `"nodes"` is optional and attaches predicates only to the vertices that need them; a vertex without an entry matches anything.
- **Parsing** goes in `utils.cpp`/`utils.hpp`. A helper `parse_pattern_template(const bjsn::object&)` numbers vertices in order of first appearance and builds `where_clause(obj["rule"])` for non-null predicates. It rejects a `"nodes"` entry that appears in no edge, then calls `validate()`.
- **Finishing:** report `lcc_stats` through the return value and finish with `clip.update_selectors(mg.get_selector_info())`.

### Room for NLCC later
Right now the `lcc_vertex` state and the round loop sit inside `local_constraint_check`. When NLCC is added, move them into a shared helper so a `prune_pattern` driver can alternate LCC and NLCC the way Alg. 1 does, without persisting ω. If ω ever needs to be inspected, it can be added as an optional output.

## Verification
- **Build:** `cmake --build build`.
- **Tests:** add `test/clippy/tests/test_mg_lcc.py`, using the `metallgraph` fixture from `test/clippy/tests/conftest.py`. Test vertex labels are set with `mg.assign("node.lbl", "X", where=mg.node.id == "...")`. Cases:
  - **Triangle, wildcard vertices:** triangle edges survive; path-graph edges and pendant edges such as 2tri-c/2tri-d are false.
  - **Labeled path a–b–c with unique labels** (acyclic, unique labels, so LCC is exact): survivors exactly equal the union of matches.
  - **Multiplicity:** a star with center A and two B leaves. A vertex labeled A with only one B neighbor is pruned, and so are its edges.
  - **Edge predicate:** a template edge with `mg.edge.weight > 2` removes edges that fail it, including the case where a parallel edge fails.
  - **Iterative pruning:** a case where removing one vertex leads to more removals in later rounds, so more than one round runs.
  - **`where` scoping:** restricting with `where=mg.edge.graphnum == 3` restricts survivors.
  - **Node recovery:** `select_nodes(where=mg.edge.m)` returns the surviving vertices.
  - **Errors:** an empty template, a disconnected template, a self-loop, an unknown vertex name, more than 64 vertices, and an existing output series.
  - **Recall oracle:** a small Python brute-force subgraph-isomorphism enumerator over the test graph checks that the LCC survivors are a superset of the exact solution, for several templates.
- **Run:** `cd test/clippy && pytest tests/test_mg_lcc.py`, then the full `pytest tests` and a multi-rank run (e.g. `mpirun -n 4`) to exercise the async paths.
