from conftest import is_as_selected


def test_mg_sample_edges(metallgraph):
    metallgraph.sample_edges(k=10, series_name="samp1")
    select_data = metallgraph.select_edges(where=metallgraph.edge.samp1 == True)
    is_as_selected(select_data, {"samp1": True}, ["u", "v"], [])
    assert len(select_data) == 10

    metallgraph.sample_edges(k=3, series_name="samp2", where=metallgraph.edge.graphnum == 1)
    select_data = metallgraph.select_edges(where=metallgraph.edge.samp2 == True)
    is_as_selected(select_data, {"samp2": True, "graphnum": 1}, ["u", "v"], [])
    assert len(select_data) == 3
