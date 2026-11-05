# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest
from clippy import MetallGraph  # type: ignore
from clippy.backends.fs.execution import NonZeroReturnCodeError  # type: ignore
from conftest import DATA_DIR

# Edge lists come from data/metall_graph/csv/*.csv (s -> t). All edges in the
# reference graph are directed.
FIVE_CLIQUE = [f"5clique-{c}" for c in "abcde"]  # x -> y for every x < y
FOUR_CLIQUE = [f"4clique-{c}" for c in "abcd"]  # x -> y for every x < y
PATH = [f"path-{c}" for c in "abcdefg"]  # a -> b -> ... -> g
# a->b, b->c, c->a, c->d, d->e, e->c
TWO_TRIANGLES_IN = {"2tri-a": 1, "2tri-b": 1, "2tri-c": 2, "2tri-d": 1, "2tri-e": 1}
TWO_TRIANGLES_OUT = {"2tri-a": 1, "2tri-b": 1, "2tri-c": 2, "2tri-d": 1, "2tri-e": 1}
# undirected: each incident edge adds 1 to both in- and out-degree
TWO_TRIANGLES_UNDIRECTED = {
    "2tri-a": 2,
    "2tri-b": 2,
    "2tri-c": 4,
    "2tri-d": 2,
    "2tri-e": 2,
}


def clique_in(nodes):
    return {n: i for i, n in enumerate(nodes)}


def clique_out(nodes):
    return {n: len(nodes) - 1 - i for i, n in enumerate(nodes)}


def path_in(nodes):
    return {n: 0 if i == 0 else 1 for i, n in enumerate(nodes)}


def path_out(nodes):
    return {n: 0 if i == len(nodes) - 1 else 1 for i, n in enumerate(nodes)}


REFERENCE_IN = {
    **clique_in(FIVE_CLIQUE),
    **clique_in(FOUR_CLIQUE),
    **path_in(PATH),
    **TWO_TRIANGLES_IN,
}
REFERENCE_OUT = {
    **clique_out(FIVE_CLIQUE),
    **clique_out(FOUR_CLIQUE),
    **path_out(PATH),
    **TWO_TRIANGLES_OUT,
}


def values_by_id(mg, series):
    """Returns {node id: value} for every node that has a value in `series`."""
    rows = mg.select_nodes(series_names=[mg.node.id, getattr(mg.node, series)])
    key = f"node.{series}"
    return {r["node.id"]: r[key] for r in rows if key in r}


@pytest.fixture()
def empty_graph(tmp_path):
    return MetallGraph(str(tmp_path / "metallgraph.db"))


def test_mg_indegree(metallgraph):
    metallgraph.in_degree("indeg")
    assert values_by_id(metallgraph, "indeg") == REFERENCE_IN


def test_mg_outdegree(metallgraph):
    metallgraph.out_degree("outdeg")
    assert values_by_id(metallgraph, "outdeg") == REFERENCE_OUT


def test_mg_degree_qualified_name(metallgraph):
    metallgraph.in_degree("node.indeg")
    assert values_by_id(metallgraph, "indeg") == REFERENCE_IN


def test_mg_degree_node_where(metallgraph):
    # node.gnum == 3 is set on the path nodes by the fixture. Only nodes that
    # match get a value, and only edges with both endpoints matching count.
    metallgraph.in_degree("indeg", where=metallgraph.node.gnum == 3)
    assert values_by_id(metallgraph, "indeg") == path_in(PATH)

    metallgraph.out_degree("outdeg", where=metallgraph.node.gnum == 3)
    assert values_by_id(metallgraph, "outdeg") == path_out(PATH)


def test_mg_degree_edge_where(metallgraph):
    # Nodes incident to a matching edge get a value; only matching edges count.
    metallgraph.in_degree("indeg", where=metallgraph.edge.graphnum == 2)
    assert values_by_id(metallgraph, "indeg") == clique_in(FIVE_CLIQUE)

    metallgraph.out_degree("outdeg", where=metallgraph.edge.graphnum == 2)
    assert values_by_id(metallgraph, "outdeg") == clique_out(FIVE_CLIQUE)


def test_mg_indegree_induced_subgraph(metallgraph):
    # indeg > 2 selects 5clique-d (3), 5clique-e (4) and 4clique-d (3). The only
    # edge among them is 5clique-d -> 5clique-e.
    metallgraph.in_degree("indeg")
    metallgraph.in_degree("indeg2", where=metallgraph.node.indeg > 2)
    assert values_by_id(metallgraph, "indeg2") == {
        "5clique-d": 0,
        "5clique-e": 1,
        "4clique-d": 0,
    }


def test_mg_outdegree_induced_subgraph(metallgraph):
    # outdeg > 2 selects 5clique-a (4), 5clique-b (3) and 4clique-a (3). The only
    # edge among them is 5clique-a -> 5clique-b.
    metallgraph.out_degree("outdeg")
    metallgraph.out_degree("outdeg2", where=metallgraph.node.outdeg > 2)
    assert values_by_id(metallgraph, "outdeg2") == {
        "5clique-a": 1,
        "5clique-b": 0,
        "4clique-a": 0,
    }


def test_mg_degree_undirected(empty_graph):
    empty_graph.ingest_parquet_edges(
        DATA_DIR + "/pq/two_triangles_0.parquet", "s", "t", directed=False
    )
    empty_graph.in_degree("indeg")
    empty_graph.out_degree("outdeg")
    assert values_by_id(empty_graph, "indeg") == TWO_TRIANGLES_UNDIRECTED
    assert values_by_id(empty_graph, "outdeg") == TWO_TRIANGLES_UNDIRECTED


def test_mg_degree_mixed_directedness(empty_graph):
    # directedness is per edge, so both kinds can live in one graph
    empty_graph.ingest_parquet_edges(
        DATA_DIR + "/pq/two_triangles_0.parquet", "s", "t", directed=False
    )
    empty_graph.ingest_parquet_edges(
        DATA_DIR + "/pq/four_clique_0.parquet", "s", "t", directed=True
    )
    empty_graph.in_degree("indeg")
    empty_graph.out_degree("outdeg")
    assert values_by_id(empty_graph, "indeg") == {
        **TWO_TRIANGLES_UNDIRECTED,
        **clique_in(FOUR_CLIQUE),
    }
    assert values_by_id(empty_graph, "outdeg") == {
        **TWO_TRIANGLES_UNDIRECTED,
        **clique_out(FOUR_CLIQUE),
    }


def test_mg_degree_isolated_node(empty_graph):
    # 2tri-f appears only in the node file, so it has no edges
    empty_graph.ingest_parquet_edges(DATA_DIR + "/pq/two_triangles_0.parquet", "s", "t")
    empty_graph.ingest_parquet_nodes(
        DATA_DIR + "/pq/two_triangles-nodes_0.parquet", "id", add_new=True
    )
    empty_graph.in_degree("indeg")
    empty_graph.out_degree("outdeg")
    assert values_by_id(empty_graph, "indeg") == {**TWO_TRIANGLES_IN, "2tri-f": 0}
    assert values_by_id(empty_graph, "outdeg") == {**TWO_TRIANGLES_OUT, "2tri-f": 0}


def test_mg_degree_error_series_exists(metallgraph):
    with pytest.raises(NonZeroReturnCodeError, match="already exists"):
        metallgraph.in_degree("gnum")
    with pytest.raises(NonZeroReturnCodeError, match="already exists"):
        metallgraph.out_degree("gnum")


def test_mg_degree_error_edge_series_name(metallgraph):
    with pytest.raises(NonZeroReturnCodeError, match="Invalid node series name"):
        metallgraph.in_degree("edge.indeg")
    with pytest.raises(NonZeroReturnCodeError, match="Invalid node series name"):
        metallgraph.out_degree("edge.outdeg")
