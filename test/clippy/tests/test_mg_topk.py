# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest


@pytest.fixture()
def topk_graph(metallgraph):
    metallgraph.add_faker("node.randpct", "percentage")
    return metallgraph


def test_mg_topk(topk_graph):
    mg = topk_graph

    results = mg.topk(mg.edge.randint, k=5, addl_series=[mg.edge.name])
    assert len(results) == 5
    curr_max = 10000
    for result in results:
        assert len(result) == 2
        assert result[0] <= curr_max
        curr_max = result[0]

    results = mg.topk(mg.node.randpct, k=50000)
    assert len(results) == mg.describe()["nv"]
    curr_max = 10000
    for result in results:
        assert len(result) == 1
        assert result[0] <= curr_max
        curr_max = result[0]

    results = mg.topk(mg.edge.randint, where=mg.edge.graphnum == 1)
    rresults = [x[0] for x in results]
    assert rresults == [92, 84, 76, 57, 33, 19]
