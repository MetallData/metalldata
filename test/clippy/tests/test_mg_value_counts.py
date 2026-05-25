def test_mg_value_counts_edge(metallgraph):
    results = metallgraph.value_counts(metallgraph.edge.graphnum, k=100)
    assert len(results) == 4
    assert sum(r[1] for r in results) == 28
    counts = {r[0]: r[1] for r in results}
    assert counts[2] == 10


def test_mg_value_counts_top_bottom_k(metallgraph):
    results_top = metallgraph.value_counts(metallgraph.edge.graphnum, k=2)
    assert len(results_top) == 2

    results_bottom = metallgraph.value_counts(metallgraph.edge.graphnum, k=-2)
    assert len(results_bottom) == 2

    top_vals = {r[0] for r in results_top}
    bottom_vals = {r[0] for r in results_bottom}
    assert 2 in top_vals
    assert 2 not in bottom_vals


def test_mg_value_counts_where(metallgraph):
    results = metallgraph.value_counts(
        metallgraph.edge.graphnum, k=100, where=metallgraph.edge.graphnum == 0
    )
    assert len(results) == 1
    assert results[0][0] == 0
    assert results[0][1] == 6


def test_mg_value_counts_bool(metallgraph):
    results = metallgraph.value_counts(metallgraph.edge.relevant, k=100)
    assert len(results) == 2
    counts = {r[0]: r[1] for r in results}
    assert counts[True] == 12
    assert counts[False] == 16


def test_mg_value_counts_node(metallgraph):
    results = metallgraph.value_counts(metallgraph.node.gnum, k=100)
    counts = {r[0]: r[1] for r in results}
    assert counts[3] == 7
    assert counts[None] == 14
