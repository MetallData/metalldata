# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from conftest import is_as_selected, is_specific


def test_mg_nhops(metallgraph):
    metallgraph.nhops("nhops", 2, ["path-a", "path-f"])
    select_data = metallgraph.select_nodes()
    required_result = {
        "path-a": {"node.nhops": 0},
        "path-f": {"node.nhops": 0},
        "path-b": {"node.nhops": 1},
        "path-g": {"node.nhops": 1},
        "path-c": {"node.nhops": 2},
    }

    is_specific(select_data, "node.id", required_result)

    # Every other node (including path-d and path-e, 3+ hops away) is unreached.
    unreached = [d for d in select_data if d["node.id"] not in required_result]
    assert len(unreached) == 21 - len(required_result)
    is_as_selected(unreached, {}, ["node.id"], ["node.nhops"])
