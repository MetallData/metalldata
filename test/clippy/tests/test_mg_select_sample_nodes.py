# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from conftest import is_as_selected


def test_mg_select_sample_nodes(metallgraph):
    for _ in range(20):
        sample_data = metallgraph.select_sample_nodes(
            k=10, series_names=[metallgraph.node.id]
        )
        assert len(sample_data) == 10
        is_as_selected(sample_data, {}, ["node.id"], [])

    for _ in range(5):
        sample_data = metallgraph.select_sample_nodes(
            k=5,
            series_names=[metallgraph.node.id, metallgraph.node.gnum],
            where=metallgraph.node.gnum == 3,
        )
        assert len(sample_data) == 5
        for el in sample_data:
            assert el["node.gnum"] == 3
        is_as_selected(sample_data, {"node.gnum": 3}, ["node.id"], [])

    for _ in range(5):
        sample_data = metallgraph.select_sample_nodes(
            k=1000,
            series_names=[metallgraph.node.id, metallgraph.node.gnum],
            where=metallgraph.node.gnum == 3,
        )
        total_nodes_g3 = len(metallgraph.select_nodes(where=metallgraph.node.gnum == 3))
        assert len(sample_data) <= total_nodes_g3
        is_as_selected(sample_data, {"node.gnum": 3}, ["node.id"], [])

    for _ in range(5):
        total_nodes = metallgraph.describe()["nv"]
        sample_data = metallgraph.select_sample_nodes(k=total_nodes + 10)
        assert len(sample_data) <= total_nodes

    for seed in range(5):
        sd1 = metallgraph.select_sample_nodes(
            10, series_names=[metallgraph.node.id, metallgraph.node.gnum], seed=seed
        )
        sd2 = metallgraph.select_sample_nodes(
            10, series_names=[metallgraph.node.id, metallgraph.node.gnum], seed=seed
        )
        sd3 = metallgraph.select_sample_nodes(
            10, series_names=[metallgraph.node.id, metallgraph.node.gnum], seed=seed + 1
        )

        setres_sd1 = set((k, v) for d in sd1 for k, v in d.items())
        setres_sd2 = set((k, v) for d in sd2 for k, v in d.items())
        setres_sd3 = set((k, v) for d in sd3 for k, v in d.items())
        assert len(sd1) == len(sd2) == len(sd3)
        assert setres_sd1 == setres_sd2
        assert setres_sd2 != setres_sd3
