# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from conftest import is_as_selected


def test_mg_select_edges(metallgraph):
    
    selected = metallgraph.select_edges(
        series_names=[metallgraph.edge.u, metallgraph.edge.v], limit=28
    )
    assert len(selected) == 28
    is_as_selected(selected, {}, ["edge.u", "edge.v"], [])

    
    selected = metallgraph.select_edges(
        series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum],
        where=metallgraph.edge.graphnum == 0, limit=5
    )
    assert len(selected) == 5
    for el in selected:
        assert el["edge.graphnum"] == 0
    is_as_selected(selected, {"edge.graphnum": 0}, ["edge.u", "edge.v"], [])

    
    selected = metallgraph.select_edges(
        series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum],
        where=metallgraph.edge.graphnum == 1,
    )
    is_as_selected(selected, {"edge.graphnum": 1}, ["edge.u", "edge.v"], [])
