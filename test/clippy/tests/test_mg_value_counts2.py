# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT


def test_mg_value_counts2_edge(metallgraph):
    # edge.graphnum value_counts is {2: 10, 1: 6, 3: 6, 0: 6}, so the count
    # "10" occurs once and the count "6" occurs three times.
    results = metallgraph.value_counts2(metallgraph.edge.graphnum)
    counts = {r[0]: r[1] for r in results}
    assert counts[10] == 1
    assert counts[6] == 3


def test_mg_value_counts2_where(metallgraph):
    results = metallgraph.value_counts2(
        metallgraph.edge.graphnum, where=metallgraph.edge.graphnum == 0
    )
    counts = {r[0]: r[1] for r in results}
    assert len(counts) == 1
    assert counts[6] == 1


def test_mg_value_counts2_bool(metallgraph):
    # edge.relevant value_counts is {True: 12, False: 16}, two distinct
    # counts, each occurring once.
    results = metallgraph.value_counts2(metallgraph.edge.relevant)
    counts = {r[0]: r[1] for r in results}
    assert len(counts) == 2
    assert counts[12] == 1
    assert counts[16] == 1


def test_mg_value_counts2_node(metallgraph):
    # node.gnum value_counts is {3: 7, None: 14}, two distinct counts, each
    # occurring once.
    results = metallgraph.value_counts2(metallgraph.node.gnum)
    counts = {r[0]: r[1] for r in results}
    assert len(counts) == 2
    assert counts[7] == 1
    assert counts[14] == 1
