from conftest import is_as_selected


def test_mg_cc_full_graph(metallgraph):
    metallgraph.connected_components("cc")
    select_data = metallgraph.select_nodes()

    cc_by_id = {d["node.id"]: d["node.cc"] for d in select_data}

    path_nodes = ["path-a", "path-b", "path-c", "path-d", "path-e", "path-f", "path-g"]
    path_ccs = {cc_by_id[n] for n in path_nodes}
    assert len(path_ccs) == 1

    clique_nodes = ["5clique-a", "5clique-b", "5clique-c", "5clique-d", "5clique-e"]
    clique_ccs = {cc_by_id[n] for n in clique_nodes}
    assert len(clique_ccs) == 1

    assert path_ccs.isdisjoint(clique_ccs)

    all_ccs = {cc for cc in cc_by_id.values()}
    # four_clique, five_clique, two_triangles and path_graph each form 1 component
    assert len(all_ccs) == 4


def test_mg_cc_where(metallgraph):
    metallgraph.connected_components("cc", where=metallgraph.edge.graphnum == 3)
    select_data = metallgraph.select_nodes(where=metallgraph.edge.graphnum == 3)

    # Exclude destination-only nodes that get no cc in directed graphs
    cc_vals = {d["node.cc"] for d in select_data}
    assert len(cc_vals) == 1
