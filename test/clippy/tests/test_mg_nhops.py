# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from conftest import is_as_selected, is_specific


def test_mg_nhops(metallgraph):
    metallgraph.nhops("nhops", 2, ["path-a", "path-f"])
    select_data = metallgraph.select_nodes()
    required_result = {
        "path-a": {"nhops": 0},
        "path-f": {"nhops": 0},
        "path-b": {"nhops": 1},
        "path-g": {"nhops": 1},
        "path-c": {"nhops": 2},
    }

    for d in select_data:
        if "nhops" in d:
            assert d["nhops"] <= 2

    is_specific(select_data, "id", required_result)
    select_data = metallgraph.select_nodes(where=metallgraph.node.gnum != 3)
    is_as_selected(select_data, {}, ["id"], ["nhops"])
