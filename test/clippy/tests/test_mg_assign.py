from conftest import is_as_selected


def test_mg_assign(metallgraph):
    metallgraph.assign("node.assign1", True)
    select_data = metallgraph.select_nodes()
    is_as_selected(select_data, {}, ["id", "assign1"], ["field_does_not_exist"])
    metallgraph.assign("edge.assign1", True)
    select_data = metallgraph.select_edges()
    is_as_selected(select_data, {"assign1": True}, ["u", "v"], ["field_does_not_exist"])

    metallgraph.assign("edge.assign2", True, where=metallgraph.edge.graphnum == 0)
    select_data = metallgraph.select_edges(where=metallgraph.edge.graphnum == 0)
    is_as_selected(select_data, {"assign2": True}, ["u", "v"], ["field_does_not_exist"])
    select_data = metallgraph.select_edges(where=metallgraph.edge.graphnum != 0)
    is_as_selected(select_data, {}, ["u", "v"], ["field_does_not_exist", "assign2"])

    metallgraph.assign("node.gnum", 3, where=metallgraph.edge.graphnum == 3)
    select_data = metallgraph.select_nodes(where=metallgraph.edge.graphnum == 3)
    is_as_selected(select_data, {"gnum": 3}, [], [])
    select_data = metallgraph.select_nodes(where=metallgraph.edge.graphnum != 3)
    is_as_selected(select_data, {}, [], ["gnum"])
