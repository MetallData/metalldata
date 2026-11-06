# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

# Tests for local_constraint_check (PruneJuice LCC).
#
# The `metallgraph` fixture graph has 21 nodes and 28 edges in four connected
# components. LCC treats every edge as undirected.
#
#   two_triangles, a "bowtie" (edge.graphnum == 0):
#
#       2tri-a                 2tri-d
#         |   \               /   |
#         |     \           /     |
#         |       2tri-c          |
#         |     /           \     |
#         |   /               \   |
#       2tri-b                 2tri-e
#
#   four_clique (edge.graphnum == 1), every pair connected:
#
#       4clique-a ------- 4clique-b
#           |   \         /   |
#           |     \     /     |
#           |       \ /       |
#           |       / \       |
#           |     /     \     |
#           |   /         \   |
#       4clique-c ------- 4clique-d
#
#   five_clique (edge.graphnum == 2): 5clique-a .. 5clique-e, every pair
#   connected (10 edges).
#
#   path_graph (edge.graphnum == 3):
#
#       path-a -- path-b -- path-c -- path-d -- path-e -- path-f -- path-g
#
#   The fixture also sets node.gnum == 3 on the seven path nodes; no other
#   node has node.gnum.
#
# Edge colors (edge.color):
#   bowtie:       a-b, b-c, c-a blue;   c-d, d-e, e-c red
#   four_clique:  a-b, b-c, c-d black;  a-c, b-d orange;  a-d has no color
#   five_clique:  a-c, b-c, c-d red;    a-d, b-d, c-e green;
#                 a-b, a-e, b-e, d-e grey
#   path:         a-b red, b-c orange, c-d yellow, d-e green, e-f blue,
#                 f-g indigo

import itertools

import pytest
from clippy.backends.fs.execution import NonZeroReturnCodeError  # type: ignore


def jl(rule) -> dict:
    """Wraps a raw jsonlogic rule the way clippy serializes expressions."""
    return {"expression_type": "jsonlogic", "rule": rule}


def ids(*names) -> dict:
    """Vertex predicate matching nodes whose id is one of `names`."""
    return jl({"in": [{"var": "node.id"}, list(names)]})


def edge_pairs(mg, where=None):
    kw = {"series_names": [mg.edge.u, mg.edge.v], "limit": 100000}
    if where is not None:
        kw["where"] = where
    return {frozenset((e["edge.u"], e["edge.v"])) for e in mg.select_edges(**kw)}


def surviving(mg, out="m"):
    return edge_pairs(mg, where=getattr(mg.edge, out))


def pairs(*ps):
    return {frozenset(p.split()) for p in ps}


BOWTIE = pairs("2tri-a 2tri-b", "2tri-b 2tri-c", "2tri-c 2tri-a",
               "2tri-c 2tri-d", "2tri-d 2tri-e", "2tri-e 2tri-c")
CLIQUE4 = {frozenset(p) for p in itertools.combinations(
    [f"4clique-{c}" for c in "abcd"], 2)}
CLIQUE5 = {frozenset(p) for p in itertools.combinations(
    [f"5clique-{c}" for c in "abcde"], 2)}

TRIANGLE = {"edges": [["a", "b"], ["b", "c"], ["c", "a"]]}


def exact_match_edges(edges, tmpl_nodes, tmpl_edges):
    """Brute-force union of edges in exact matches (wildcard templates only)."""
    adj = {}
    for e in edges:
        u, v = tuple(e)
        adj.setdefault(u, set()).add(v)
        adj.setdefault(v, set()).add(u)
    result = set()
    for verts in itertools.permutations(adj, len(tmpl_nodes)):
        phi = dict(zip(tmpl_nodes, verts))
        if all(phi[b] in adj[phi[a]] for a, b in tmpl_edges):
            result |= {frozenset((phi[a], phi[b])) for a, b in tmpl_edges}
    return result


ALL_EDGES = BOWTIE | CLIQUE4 | CLIQUE5 | pairs(
    "path-a path-b", "path-b path-c", "path-c path-d",
    "path-d path-e", "path-e path-f", "path-f path-g")
RED_EDGES = pairs("2tri-c 2tri-d", "2tri-d 2tri-e", "2tri-e 2tri-c",
                  "5clique-a 5clique-c", "5clique-b 5clique-c",
                  "5clique-c 5clique-d", "path-a path-b")


#
# Basic usage
#

def test_lcc_single_edge_template_keeps_everything(metallgraph):
    # Template:   a -- b      (no predicates)
    #
    # Every edge in the graph (see the drawing at the top of the file) is an
    # a -- b, so all 28 edges and all 21 nodes survive, and nothing is
    # pruned in the first round.
    stats = metallgraph.local_constraint_check(
        "m", {"edges": [["a", "b"]]})
    assert surviving(metallgraph) == ALL_EDGES
    assert stats == {"rounds": 1, "num_nodes": 21, "num_edges": 28}


def test_lcc_output_series(metallgraph):
    # Template:     a
    #              / \
    #             b - c       (TRIANGLE, no predicates)
    #
    # The bowtie, four_clique and five_clique are all made of triangles, so
    # their 6 + 6 + 10 = 22 edges survive. The path has no triangle, so its
    # 6 edges are pruned:
    #
    #   path-a -- path-b -- path-c -- path-d -- path-e -- path-f -- path-g
    #         false     false     false     false     false     false
    #
    # The output is a bool edge series. A bare name gets the "edge" prefix,
    # every in-scope edge gets a value (true or false), and the series shows
    # up as a selector afterwards. A node series name is rejected.
    metallgraph.local_constraint_check("m", TRIANGLE)
    assert metallgraph.describe()["nonnull_edge_count"]["edge.m"] == 28
    assert len(surviving(metallgraph)) == 22

    metallgraph.local_constraint_check("edge.m2", TRIANGLE)
    assert surviving(metallgraph, "m2") == surviving(metallgraph)

    with pytest.raises(NonZeroReturnCodeError, match="Invalid edge series"):
        metallgraph.local_constraint_check("node.m3", TRIANGLE)


def test_lcc_none_and_empty_predicates_are_wildcards(metallgraph):
    # Template:     a
    #              / \
    #             b - c       (TRIANGLE)
    #
    # Omitting "nodes", a None predicate, and {} all mean "match anything",
    # so spelling the triangle's predicates three different ways gives the
    # same 22 surviving edges as the plain TRIANGLE.
    metallgraph.local_constraint_check("m", TRIANGLE)
    metallgraph.local_constraint_check(
        "m2", {"nodes": {"a": None, "b": {}},
               "edges": [["a", "b", None], ["b", "c", {}], ["c", "a"]]})
    assert surviving(metallgraph, "m2") == surviving(metallgraph)


def test_lcc_edge_predicate(metallgraph):
    # Template:   a --[edge.color == "red"]-- b
    #
    # An edge predicate is a jsonlogic expression (written here with clippy
    # selectors) restricting which background edges a template edge matches.
    # The red edges in the graph are:
    #
    #   bowtie:        2tri-c --red-- 2tri-d --red-- 2tri-e --red-- 2tri-c
    #
    #   five_clique:   5clique-a --red-- 5clique-c --red-- 5clique-b
    #                                       |
    #                                      red
    #                                       |
    #                                   5clique-d
    #
    #   path:          path-a --red-- path-b
    #
    # Each red edge is an a -- b match on its own, so exactly these 7 edges
    # survive.
    metallgraph.local_constraint_check(
        "m", {"edges": [["a", "b", metallgraph.edge.color == "red"]]})
    assert surviving(metallgraph) == RED_EDGES


def test_lcc_vertex_predicate(metallgraph):
    # Template:   a -- b      with  a: node.gnum == 3
    #
    # A vertex predicate is a jsonlogic expression (written here with clippy
    # selectors) restricting which background nodes a template vertex
    # matches. Only the path nodes have node.gnum == 3:
    #
    #   path-a -- path-b -- path-c -- path-d -- path-e -- path-f -- path-g
    #   gnum=3    gnum=3    gnum=3    gnum=3    gnum=3    gnum=3    gnum=3
    #
    # "a" must be a path node and "b" can be any neighbor of it. Path nodes
    # only neighbor other path nodes, so the 6 path edges survive and every
    # other component is pruned.
    metallgraph.local_constraint_check(
        "m", {"edges": [["a", "b"]], "nodes": {"a": metallgraph.node.gnum == 3}})
    assert surviving(metallgraph) == pairs(
        "path-a path-b", "path-b path-c", "path-c path-d",
        "path-d path-e", "path-e path-f", "path-f path-g")


def test_lcc_vertex_predicate_over_edge_series(metallgraph):
    # Template:   a -- b      with  a: edge.color == "indigo"
    #
    # A vertex predicate may reference edge series. As with any where clause,
    # an edge expression used to select *nodes* matches the nodes incident to
    # at least one matching edge.
    #
    # The relevant part of the graph is the end of the path:
    #
    #     path-d --green-- path-e --blue-- path-f --indigo-- path-g
    #
    # Only path-f -- path-g is indigo, so the vertex predicate
    #     "a": edge.color == "indigo"
    # means "a must be a node that touches an indigo edge": path-f or path-g.
    #
    # The predicate constrains which *nodes* can play "a", not which *edges*
    # can play the template edge a -- b. "b" has no predicate, so it can be any
    # neighbor of "a". The template edge therefore matches every edge with
    # path-f or path-g at one end:
    #   - path-f -- path-g  (a = path-f, b = path-g, or vice versa)
    #   - path-e -- path-f  (a = path-f, b = path-e). This edge is blue, but
    #     its color is irrelevant: only "a" is constrained, and path-f
    #     touches an indigo edge (to path-g).
    # path-g has no other neighbors, so nothing else survives.
    metallgraph.local_constraint_check(
        "m", {"edges": [["a", "b"]],
              "nodes": {"a": metallgraph.edge.color == "indigo"}})
    assert surviving(metallgraph) == pairs("path-e path-f", "path-f path-g")

    # Contrast: the same expression as an *edge* predicate constrains the
    # template edge itself, so only the indigo edge survives.
    metallgraph.local_constraint_check(
        "m2", {"edges": [["a", "b", metallgraph.edge.color == "indigo"]]})
    assert surviving(metallgraph, "m2") == pairs("path-f path-g")


def test_lcc_edge_predicate_over_node_series(metallgraph):
    # Template:   a --[node.gnum == 3]-- b
    #
    # An edge predicate may reference node series. As with any where clause,
    # a node expression used to select *edges* matches the edges whose two
    # endpoints both match. Only the path nodes have node.gnum == 3:
    #
    #   path-a -- path-b -- path-c -- path-d -- path-e -- path-f -- path-g
    #   gnum=3    gnum=3    gnum=3    gnum=3    gnum=3    gnum=3    gnum=3
    #
    # Every path edge has both endpoints at gnum=3, so the 6 path edges
    # survive. No other edge has even one such endpoint.
    metallgraph.local_constraint_check(
        "m", {"edges": [["a", "b", metallgraph.node.gnum == 3]]})
    assert surviving(metallgraph) == {
        e for e in ALL_EDGES if all(n.startswith("path-") for n in e)}


def test_lcc_edges_are_undirected(metallgraph):
    # Template:   a -- b      with  a: node.id == "path-g"
    #
    # The last path edge is stored with path-f as the source and path-g as
    # the target:
    #
    #     path-e -- path-f --> path-g
    #
    # LCC ignores direction, so "a" = path-g still matches the edge even
    # though path-g is its target. path-g has no other neighbors, so only
    # path-f -- path-g survives.
    metallgraph.local_constraint_check(
        "m", {"edges": [["a", "b"]],
              "nodes": {"a": metallgraph.node.id == "path-g"}})
    assert surviving(metallgraph) == pairs("path-f path-g")


def test_lcc_star_template(metallgraph):
    # Template:        x
    #                  |
    #             y - hub - z      (no predicates)
    #
    # The hub needs three *distinct* neighbors. Node degrees in the bowtie:
    #
    #     (2) 2tri-a                 2tri-d (2)
    #           |   \               /   |
    #           |     \           /     |
    #           |    (4) 2tri-c          |
    #           |     /           \     |
    #           |   /               \   |
    #     (2) 2tri-b                 2tri-e (2)
    #
    # Only 2tri-c can be the hub, so its 4 edges survive. 2tri-a -- 2tri-b
    # and 2tri-d -- 2tri-e have no hub at either end and are pruned. Every
    # clique node has degree >= 3, so both cliques survive whole. The path's
    # maximum degree is 2, so the whole path is pruned.
    metallgraph.local_constraint_check(
        "m", {"edges": [["hub", "x"], ["hub", "y"], ["hub", "z"]]})
    assert surviving(metallgraph) == pairs(
        "2tri-c 2tri-a", "2tri-c 2tri-b", "2tri-c 2tri-d", "2tri-c 2tri-e",
    ) | CLIQUE4 | CLIQUE5


def test_lcc_no_match(metallgraph):
    # Template:   a --[edge.color == "purple"]-- b
    #
    # No edge in the graph is purple (see the color table at the top of the
    # file), so nothing survives. Every edge is still written, as false.
    stats = metallgraph.local_constraint_check(
        "m", {"edges": [["a", "b", metallgraph.edge.color == "purple"]]})
    assert stats["num_nodes"] == 0 and stats["num_edges"] == 0
    assert surviving(metallgraph) == set()
    assert edge_pairs(metallgraph, where=jl({"!": {"var": "edge.m"}})) == ALL_EDGES


def test_lcc_where_node_clause(metallgraph):
    # Template:   a -- b      (no predicates)
    # where:      node.gnum == 3
    #
    # `where` limits the graph LCC runs on. A node clause keeps edges whose
    # endpoints both match, which here is just the path:
    #
    #   path-a -- path-b -- path-c -- path-d -- path-e -- path-f -- path-g
    #   gnum=3    gnum=3    gnum=3    gnum=3    gnum=3    gnum=3    gnum=3
    #
    # All 6 path edges survive. The other 22 edges are outside the scope, so
    # edge.m is left unset on them (not even false).
    metallgraph.local_constraint_check(
        "m", {"edges": [["a", "b"]]}, where=metallgraph.node.gnum == 3)
    path = {e for e in ALL_EDGES if all(n.startswith("path-") for n in e)}
    assert surviving(metallgraph) == path
    assert metallgraph.describe()["nonnull_edge_count"]["edge.m"] == 6


def test_lcc_result_feeds_other_methods(metallgraph):
    # Template:     a
    #              / \
    #             b - c       (TRIANGLE)
    #
    # As in test_lcc_output_series, the bowtie and both cliques survive and
    # the path is pruned. Passing where=edge.m to connected_components runs
    # it on just the surviving edges: 14 nodes (5 + 4 + 5) in 3 components.
    metallgraph.local_constraint_check("m", TRIANGLE)
    metallgraph.connected_components("cc", where=metallgraph.edge.m)
    ccs = {d["node.id"]: d["node.cc"] for d in metallgraph.select_nodes(
        series_names=[metallgraph.node.id, metallgraph.node.cc],
        where=metallgraph.edge.m)}
    assert len(ccs) == 14
    assert len(set(ccs.values())) == 3


#
# Algorithm behavior
#

def test_lcc_triangle_wildcard(metallgraph):
    stats = metallgraph.local_constraint_check("m", TRIANGLE)
    assert surviving(metallgraph) == BOWTIE | CLIQUE4 | CLIQUE5
    assert stats["num_edges"] == len(BOWTIE | CLIQUE4 | CLIQUE5)
    assert stats["num_nodes"] == 5 + 4 + 5
    # Non-survivors are explicitly false, so negation works.
    path = edge_pairs(metallgraph, where=jl({"!": {"var": "edge.m"}}))
    assert len(path) == 6 and all(
        all(n.startswith("path-") for n in e) for e in path)


def test_lcc_nodes_from_edges(metallgraph):
    metallgraph.local_constraint_check("m", TRIANGLE)
    nodes = {d["node.id"] for d in metallgraph.select_nodes(
        series_names=[metallgraph.node.id], where=metallgraph.edge.m)}
    assert nodes == {n for e in BOWTIE | CLIQUE4 | CLIQUE5 for n in e}


def test_lcc_labeled_path(metallgraph):
    # Unique labels on an acyclic template: LCC is exact.
    # path-b is a "B" with an "A" neighbor but no "C" neighbor.
    tmpl = {
        "nodes": {"A": ids("path-c"), "B": ids("path-b", "path-d"),
                  "C": ids("path-e")},
        "edges": [["A", "B"], ["B", "C"]],
    }
    metallgraph.local_constraint_check("m", tmpl)
    assert surviving(metallgraph) == pairs("path-c path-d", "path-d path-e")


def test_lcc_distinct_neighbors(metallgraph):
    # Star: X with two distinct Y neighbors. path-d has only one Y neighbor.
    tmpl = {
        "nodes": {"x": ids("path-d", "5clique-a"),
                  "y1": ids("path-c", "5clique-b", "5clique-c"),
                  "y2": ids("path-c", "5clique-b", "5clique-c")},
        "edges": [["x", "y1"], ["x", "y2"]],
    }
    metallgraph.local_constraint_check("m", tmpl)
    assert surviving(metallgraph) == pairs("5clique-a 5clique-b",
                                           "5clique-a 5clique-c")


def test_lcc_edge_predicate_and_rounds(metallgraph):
    light = metallgraph.edge.weight < 3
    tmpl = {"edges": [["a", "b", light], ["b", "c", light], ["c", "a", light]]}
    stats = metallgraph.local_constraint_check("m", tmpl)
    # The path is peeled from its ends over several rounds.
    assert stats["rounds"] > 3
    # 5clique keeps the light 4-cycle a-b-d-c: a known LCC false positive
    # (cycle constraints are an NLCC concern).
    assert surviving(metallgraph) == pairs(
        "2tri-a 2tri-b", "2tri-b 2tri-c", "2tri-c 2tri-a",
        "4clique-a 4clique-b", "4clique-a 4clique-c", "4clique-b 4clique-c",
        "5clique-a 5clique-b", "5clique-a 5clique-c",
        "5clique-b 5clique-d", "5clique-c 5clique-d")


def test_lcc_where(metallgraph):
    where = metallgraph.edge.graphnum == 1
    metallgraph.local_constraint_check("m", TRIANGLE, where=where)
    assert surviving(metallgraph) == CLIQUE4
    # Out-of-scope edges are left unset.
    assert edge_pairs(metallgraph, where=jl({"!": {"var": "edge.m"}})) == set()


@pytest.mark.parametrize("tmpl_edges", [
    [["a", "b"]],
    [["a", "b"], ["b", "c"]],
    [["a", "b"], ["b", "c"], ["c", "a"]],
    [["a", "b"], ["b", "c"], ["c", "d"]],
    [["a", "b"], ["a", "c"], ["a", "d"]],
    [["a", "b"], ["b", "c"], ["c", "d"], ["d", "a"]],
])
def test_lcc_recall_oracle(metallgraph, tmpl_edges):
    names = sorted({n for e in tmpl_edges for n in e})
    tmpl = {"edges": tmpl_edges}
    metallgraph.local_constraint_check("m", tmpl)
    exact = exact_match_edges(edge_pairs(metallgraph), names, tmpl_edges)
    assert exact <= surviving(metallgraph)


@pytest.mark.parametrize("tmpl,msg", [
    ({"edges": []}, "between 1 and"),
    ({"nodes": {"a": None}}, "requires an \"edges\""),
    ({"edges": [["a", "b"], ["c", "d"]]}, "connected"),
    ({"edges": [["a", "a"]]}, "self loop"),
    ({"edges": [["a", "b"]], "nodes": {"z": None}}, "not in any edge"),
    ({"edges": [["a", "b"], ["b", "a"]]}, "duplicate"),
    ({"edges": [["a", "b"]],
      "nodes": {"a": jl({"and": [{"var": "node.id"}, {"var": "edge.weight"}]})}},
     "only node or only edge"),
])
def test_lcc_bad_template(metallgraph, tmpl, msg):
    with pytest.raises(NonZeroReturnCodeError, match=msg):
        metallgraph.local_constraint_check("m", tmpl)


def test_lcc_output_exists(metallgraph):
    metallgraph.local_constraint_check("m", TRIANGLE)
    with pytest.raises(NonZeroReturnCodeError, match="already exists"):
        metallgraph.local_constraint_check("m", TRIANGLE)
