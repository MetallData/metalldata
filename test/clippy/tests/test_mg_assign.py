# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from conftest import is_as_selected


def test_mg_assign(metallgraph):
    metallgraph.assign("node.assign1", True)
    select_data = metallgraph.select_nodes()
    is_as_selected(select_data, {}, ["node.id", "node.assign1"], ["field_does_not_exist"])
    metallgraph.assign("edge.assign1", True)
    select_data = metallgraph.select_edges()
    is_as_selected(select_data, {"edge.assign1": True}, ["edge.u", "edge.v"], ["field_does_not_exist"])

    metallgraph.assign("edge.assign2", True, where=metallgraph.edge.graphnum == 0)
    select_data = metallgraph.select_edges(where=metallgraph.edge.graphnum == 0)
    is_as_selected(select_data, {"edge.assign2": True}, ["edge.u", "edge.v"], ["field_does_not_exist"])
    select_data = metallgraph.select_edges(where=metallgraph.edge.graphnum != 0)
    is_as_selected(select_data, {}, ["edge.u", "edge.v"], ["field_does_not_exist", "edge.assign2"])

    metallgraph.assign("node.gnum", 3, where=metallgraph.edge.graphnum == 3)
    select_data = metallgraph.select_nodes(where=metallgraph.edge.graphnum == 3)
    is_as_selected(select_data, {"node.gnum": 3}, [], [])
    select_data = metallgraph.select_nodes(where=metallgraph.edge.graphnum != 3)
    is_as_selected(select_data, {}, [], ["node.gnum"])
