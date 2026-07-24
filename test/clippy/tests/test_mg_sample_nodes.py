# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from conftest import is_as_selected


def test_mg_sample_nodes(metallgraph):
    metallgraph.sample_nodes(k=10, series_name="nsamp1")
    select_data = metallgraph.select_nodes(where=metallgraph.node.nsamp1 == True)
    is_as_selected(select_data, {"node.nsamp1": True}, ["node.id"], [])
    assert len(select_data) == 10

    metallgraph.sample_nodes(k=3, series_name="nsamp2", where=metallgraph.node.gnum == 3)
    select_data = metallgraph.select_nodes(where=metallgraph.node.nsamp2 == True)
    is_as_selected(select_data, {"node.nsamp2": True, "node.gnum": 3}, ["node.id"], [])
    assert len(select_data) == 3
