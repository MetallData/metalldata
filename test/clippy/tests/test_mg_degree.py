from conftest import is_specific


def test_mg_indegree(metallgraph):
    degs1 = {
        "5clique-a": {"indeg1": 0},
        "5clique-b": {"indeg1": 1},
        "5clique-c": {"indeg1": 2},
        "5clique-d": {"indeg1": 3},
        "5clique-e": {"indeg1": 4},
    }
    degs2 = {
        "5clique-d": {"indeg1": 3},
        "5clique-e": {"indeg1": 4},
    }

    metallgraph.in_degree("indeg1")
    select_data = metallgraph.select_nodes()
    is_specific(select_data, "id", degs1)

    metallgraph.in_degree("indeg2", where=metallgraph.node.indeg1 > 2)
    select_data = metallgraph.select_nodes()
    is_specific(select_data, "id", degs2)


def test_mg_outdegree(metallgraph):
    degs1 = {
        "5clique-a": {"outdeg1": 4},
        "5clique-b": {"outdeg1": 3},
        "5clique-c": {"outdeg1": 2},
        "5clique-d": {"outdeg1": 1},
        "5clique-e": {"outdeg1": 0},
    }
    degs2 = {
        "5clique-a": {"outdeg1": 4},
        "5clique-b": {"outdeg1": 3},
    }

    metallgraph.out_degree("outdeg1")
    select_data = metallgraph.select_nodes()
    is_specific(select_data, "id", degs1)

    metallgraph.out_degree("outdeg2", where=metallgraph.node.outdeg1 > 2)
    select_data = metallgraph.select_nodes()
    is_specific(select_data, "id", degs2)
