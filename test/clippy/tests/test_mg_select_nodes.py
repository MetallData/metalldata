from conftest import is_as_selected


def test_mg_select_nodes(metallgraph):
    
    selected = metallgraph.select_nodes(
        limit=10, series_names=[metallgraph.node.id]
    )
    assert len(selected) == 10
    is_as_selected(selected, {}, ["node.id"], [])

    
    selected = metallgraph.select_nodes(
        limit=5,
        series_names=[metallgraph.node.id, metallgraph.node.gnum],
        where=metallgraph.node.gnum == 3,
    )
    assert len(selected) == 5
    for el in selected:
        assert el["node.gnum"] == 3
    is_as_selected(selected, {"node.gnum": 3}, ["node.id"], [])


    selected = metallgraph.select_nodes(
        k=1000,
        series_names=[metallgraph.node.id, metallgraph.node.gnum],
        where=metallgraph.node.gnum == 3,
    )
    total_nodes_g3 = len(metallgraph.select_nodes(where=metallgraph.node.gnum == 3))
    assert len(selected) == total_nodes_g3
    is_as_selected(selected, {"node.gnum": 3}, ["node.id"], [])
