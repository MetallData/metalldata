# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest
from clippy.backends.fs.execution import NonZeroReturnCodeError  # type: ignore


def edge_keys(rows):
    return sorted((r["edge.u"], r["edge.v"]) for r in rows)


def node_ids(rows):
    return sorted(r["node.id"] for r in rows)


def test_mg_create_subgraph(metallgraph):
    mg = metallgraph
    mg.create_subgraph("g3", where=mg.edge.graphnum == 3)
    assert hasattr(mg.subgraph, "g3")

    # the hidden series do not show up as node / edge selectors or data.
    for sel in (mg.node, mg.edge):
        for name, _ in sel._hierarchy():
            assert "._" not in name
    for row in mg.select_nodes():
        assert not any(k.startswith("node._") for k in row)
    for row in mg.select_edges():
        assert not any(k.startswith("edge._") for k in row)

    expected_edges = mg.select_edges(where=mg.edge.graphnum == 3)
    expected_nodes = mg.select_nodes(where=mg.edge.graphnum == 3)
    assert len(expected_edges) > 0
    assert len(expected_edges) < len(mg.select_edges())

    assert edge_keys(mg.select_edges(where=mg.subgraph.g3)) == edge_keys(expected_edges)
    assert node_ids(mg.select_nodes(where=mg.subgraph.g3)) == node_ids(expected_nodes)
    assert edge_keys(mg.select_edges(where=mg.subgraph.g3 == True)) == edge_keys(expected_edges)

    r = mg.describe(where=mg.subgraph.g3)
    assert r["nv"] == len(expected_nodes)
    assert r["ne"] == len(expected_edges)


def test_mg_create_subgraph_no_where(metallgraph):
    mg = metallgraph
    mg.create_subgraph("everything")
    assert len(mg.select_edges(where=mg.subgraph.everything)) == len(mg.select_edges())
    assert len(mg.select_nodes(where=mg.subgraph.everything)) == len(mg.select_nodes())


def test_mg_create_subgraph_from_subgraph(metallgraph):
    mg = metallgraph
    mg.create_subgraph("g3", where=mg.edge.graphnum == 3)
    mg.create_subgraph("g3copy", where=mg.subgraph.g3)
    assert edge_keys(mg.select_edges(where=mg.subgraph.g3copy)) == edge_keys(
        mg.select_edges(where=mg.subgraph.g3)
    )


def test_mg_create_subgraph_invalid(metallgraph):
    mg = metallgraph
    mg.create_subgraph("g3", where=mg.edge.graphnum == 3)
    with pytest.raises(NonZeroReturnCodeError):
        mg.create_subgraph("g3")
    with pytest.raises(NonZeroReturnCodeError):
        mg.create_subgraph("_hidden")
    with pytest.raises(NonZeroReturnCodeError):
        mg.create_subgraph("a.b")
    # users cannot create hidden series.
    with pytest.raises(NonZeroReturnCodeError):
        mg.rename_series(mg.node.gnum, "_gnum")
    assert hasattr(mg.node, "gnum")


def test_mg_drop_subgraph(metallgraph):
    mg = metallgraph
    mg.create_subgraph("g3", where=mg.edge.graphnum == 3)
    mg.create_subgraph("g0", where=mg.edge.graphnum == 0)
    assert hasattr(mg.subgraph, "g3")
    assert hasattr(mg.subgraph, "g0")

    mg.drop_series(mg.subgraph.g3)
    assert not hasattr(mg.subgraph, "g3")
    assert hasattr(mg.subgraph, "g0")

    # the name can be reused once dropped.
    mg.create_subgraph("g3", where=mg.edge.graphnum == 0)
    assert edge_keys(mg.select_edges(where=mg.subgraph.g3)) == edge_keys(
        mg.select_edges(where=mg.subgraph.g0)
    )
