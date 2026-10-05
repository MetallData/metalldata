# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest
from clippy import MetallGraph  # type: ignore
from conftest import DATA_DIR, is_as_described


@pytest.fixture()
def empty_graph(tmp_path):
    return MetallGraph(str(tmp_path / "metallgraph.db"))


def test_mg_describe_empty(empty_graph):
    is_as_described(empty_graph, 0, 0)

    result = empty_graph.describe()
    assert result["nonnull_node_count"] == {}
    assert result["nonnull_edge_count"] == {}


def test_mg_describe_full_graph(metallgraph):
    is_as_described(metallgraph, 21, 28)

    result = metallgraph.describe()

    assert result["nonnull_node_count"] == {
        "node.id": 21,
        "node.gnum": 7,
    }
    assert result["nonnull_edge_count"] == {
        "edge.u": 28,
        "edge.v": 28,
        "edge.directed": 28,
        "edge.graphnum": 28,
        "edge.weight": 28,
        "edge.color": 27,
        "edge.name": 21,
        "edge.relevant": 28,
        "edge.randint": 28,
    }


def test_mg_describe_edge_where(metallgraph):
    result = metallgraph.describe(where=metallgraph.edge.graphnum == 0)

    assert result["nv"] == 5
    assert result["ne"] == 6

    # nodes touched by graphnum==0 edges were never assigned node.gnum, so
    # that series should be entirely absent from the count, not present
    # with a value of 0.
    assert result["nonnull_node_count"] == {"node.id": 5}
    assert result["nonnull_edge_count"] == {
        "edge.u": 6,
        "edge.v": 6,
        "edge.directed": 6,
        "edge.graphnum": 6,
        "edge.weight": 6,
        "edge.color": 6,
        "edge.name": 4,
        "edge.relevant": 6,
        "edge.randint": 6,
    }


def test_mg_describe_edge_where_matches_node_where(metallgraph):
    # edge.graphnum == 3 induces exactly the nodes that got node.gnum == 3
    # assigned in the reference graph fixture, so both where clauses should
    # describe the same induced subgraph.
    by_edge = metallgraph.describe(where=metallgraph.edge.graphnum == 3)
    by_node = metallgraph.describe(where=metallgraph.node.gnum == 3)

    for result in (by_edge, by_node):
        assert result["nv"] == 7
        assert result["ne"] == 6
        assert result["nonnull_node_count"] == {"node.id": 7, "node.gnum": 7}
        assert result["nonnull_edge_count"] == {
            "edge.u": 6,
            "edge.v": 6,
            "edge.directed": 6,
            "edge.graphnum": 6,
            "edge.weight": 6,
            "edge.color": 6,
            "edge.name": 6,
            "edge.relevant": 6,
            "edge.randint": 6,
        }
