from conftest import is_as_selected


def test_mg_add_faker(metallgraph):
    metallgraph.add_faker("node.randpct", "percentage")
    select_data = metallgraph.select_nodes()
    is_as_selected(select_data, {}, ["randpct", "id"], ["field_does_not_exist"])

    metallgraph.add_faker("node.uuid", "uuid4")
    select_data = metallgraph.select_nodes()
    is_as_selected(select_data, {}, ["uuid", "id"], ["field_does_not_exist"])

    metallgraph.add_faker("edge.temp", "double", where=metallgraph.edge.graphnum == 0)
    select_data = metallgraph.select_edges(where=metallgraph.edge.graphnum == 0)
    is_as_selected(select_data, {}, ["u", "v", "temp"], ["field_does_not_exist"])
    select_data = metallgraph.select_edges(where=metallgraph.edge.graphnum != 0)
    is_as_selected(select_data, {}, ["u", "v"], ["temp"])
