from conftest import is_as_selected


def test_mg_select_sample_edges(metallgraph):
    for _ in range(20):
        sample_data = metallgraph.select_sample_edges(
            k=10, series_names=[metallgraph.edge.u, metallgraph.edge.v]
        )
        assert len(sample_data) == 10
        is_as_selected(sample_data, {}, ["edge.u", "edge.v"], [])

    for _ in range(5):
        sample_data = metallgraph.select_sample_edges(
            k=5,
            series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum],
            where=metallgraph.edge.graphnum == 0,
        )
        assert len(sample_data) == 5
        for el in sample_data:
            assert el["edge.graphnum"] == 0
        is_as_selected(sample_data, {"edge.graphnum": 0}, ["edge.u", "edge.v"], [])

    for _ in range(5):
        sample_data = metallgraph.select_sample_edges(
            k=1000,
            series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum],
            where=metallgraph.edge.graphnum == 1,
        )
        total_edges_g1 = len(metallgraph.select_edges(where=metallgraph.edge.graphnum == 1))
        assert len(sample_data) <= total_edges_g1
        is_as_selected(sample_data, {"edge.graphnum": 1}, ["edge.u", "edge.v"], [])

    for _ in range(5):
        total_edges = metallgraph.describe()["ne"]
        sample_data = metallgraph.select_sample_edges(k=total_edges + 10)
        assert len(sample_data) <= total_edges

    for seed in range(5):
        sd1 = metallgraph.select_sample_edges(
            10, series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum], seed=seed
        )
        sd2 = metallgraph.select_sample_edges(
            10, series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum], seed=seed
        )
        sd3 = metallgraph.select_sample_edges(
            10, [metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum], seed=seed + 1
        )

        setres_sd1 = set((k, v) for d in sd1 for k, v in d.items())
        setres_sd2 = set((k, v) for d in sd2 for k, v in d.items())
        setres_sd3 = set((k, v) for d in sd3 for k, v in d.items())
        assert len(sd1) == len(sd2) == len(sd3)
        assert setres_sd1 == setres_sd2
        assert setres_sd2 != setres_sd3
