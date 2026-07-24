# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest


@pytest.fixture()
def nunique_graph(metallgraph):
    metallgraph.sample_edges(k=10, series_name="samp1")
    return metallgraph


def test_mg_nunique_edge(nunique_graph):
    mg = nunique_graph

    result = mg.nunique(series_names=["edge.graphnum"])
    assert result["edge.graphnum"] == 4

    result = mg.nunique(series_names=["edge.color"])
    assert result["edge.color"] == 8

    result = mg.nunique(series_names=["edge.relevant"])
    assert result["edge.relevant"] == 2

    result = mg.nunique(series_names=["edge.samp1"])
    assert result["edge.samp1"] == 1

    result = mg.nunique(series_names=["edge.randint"])
    assert result["edge.randint"] == 22

    result = mg.nunique(series_names=["edge.graphnum", "edge.color", "edge.relevant"])
    assert result["edge.graphnum"] == 4
    assert result["edge.color"] == 8
    assert result["edge.relevant"] == 2


def test_mg_nunique_edge_where(metallgraph):
    result = metallgraph.nunique(
        series_names=["edge.graphnum"], where=metallgraph.edge.graphnum == 0
    )
    assert result["edge.graphnum"] == 1

    result = metallgraph.nunique(
        series_names=["edge.color"], where=metallgraph.edge.graphnum == 0
    )
    assert result["edge.color"] == 2

    result = metallgraph.nunique(
        series_names=["edge.relevant"], where=metallgraph.edge.graphnum == 0
    )
    assert result["edge.relevant"] == 1


def test_mg_nunique_node(metallgraph):
    result = metallgraph.nunique(series_names=["node.id"])
    assert result["node.id"] == 21

    result = metallgraph.nunique(series_names=["node.gnum"])
    assert result["node.gnum"] == 1


def test_mg_nunique_mixed_where(metallgraph):
    result = metallgraph.nunique(
        series_names=["edge.graphnum", "edge.color", "node.id", "node.gnum"],
        where=metallgraph.edge.graphnum == 3,
    )
    assert result["edge.graphnum"] == 1
    assert result["edge.color"] == 6
    assert result["node.id"] == 7
    assert result["node.gnum"] == 1
