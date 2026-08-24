# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest
from clippy import MetallGraph  # type: ignore
from conftest import DATA_DIR, is_as_described, is_as_selected


@pytest.fixture()
def empty_graph(tmp_path):
    return MetallGraph(str(tmp_path / "metallgraph.db"))


def test_mg_ingest_parquet(empty_graph):
    empty_graph.ingest_parquet_edges(DATA_DIR + "/test", "s", "t")
    is_as_described(empty_graph, 21, 28)
    el = empty_graph.select_edges()
    is_as_selected(
        el,
        {},
        ["edge.u", "edge.v", "edge.graphnum", "edge.relevant"],
        ["foo", "field_does_not_exist"],
    )


def test_mg_ingest_parquet_nodes(empty_graph):
    edge_result = empty_graph.ingest_parquet_edges(
        DATA_DIR + "/pq/two_triangles_0.parquet", "s", "t"
    )
    assert edge_result == {
        "num_edges_ingested": 6,
        "num_new_nodes_ingested": 5,
    }

    result = empty_graph.ingest_parquet_nodes(
        DATA_DIR + "/pq/two_triangles-nodes_0.parquet",
        "id",
        add_new=True,
        metadata=["age", "grade", "label"],
    )

    assert result == {"num_nodes_ingested": 6, "num_new_nodes_ingested": 1}
    is_as_described(empty_graph, 6, 6)

    nodes = empty_graph.select_nodes(
        series_names=[
            empty_graph.node.id,
            empty_graph.node.age,
            empty_graph.node.grade,
            empty_graph.node.label,
        ]
    )
    assert {node["node.id"]: node for node in nodes} == {
        "2tri-a": {
            "node.id": "2tri-a",
            "node.age": 27,
            "node.grade": 99,
            "node.label": "alpha",
        },
        "2tri-b": {
            "node.id": "2tri-b",
            "node.age": 17,
            "node.grade": 67,
            "node.label": "bravo",
        },
        "2tri-c": {
            "node.id": "2tri-c",
            "node.age": 55,
            "node.grade": 22,
            "node.label": "charlie",
        },
        "2tri-d": {
            "node.id": "2tri-d",
            "node.age": 26,
            "node.grade": 88,
            "node.label": "delta",
        },
        "2tri-e": {
            "node.id": "2tri-e",
            "node.age": 30,
            "node.grade": 90,
            "node.label": "echo",
        },
        "2tri-f": {
            "node.id": "2tri-f",
            "node.age": 8,
            "node.grade": 3,
            "node.label": "foxtrot",
        },
    }
