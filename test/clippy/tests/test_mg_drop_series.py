import pytest
from conftest import is_as_selected


@pytest.fixture()
def drop_graph(metallgraph):
    metallgraph.assign("node.assign1", True)
    metallgraph.assign("edge.assign1", True)
    return metallgraph


def test_mg_drop_series(drop_graph):
    mg = drop_graph
    select_data = mg.select_nodes()
    is_as_selected(select_data, {}, ["node.id", "node.assign1"], ["field_does_not_exist"])
    mg.drop_series(mg.node.assign1)
    select_data = mg.select_nodes()
    is_as_selected(select_data, {}, ["node.id"], ["node.assign1", "field_does_not_exist"])

    select_data = mg.select_edges()
    is_as_selected(select_data, {}, ["edge.u", "edge.assign1"], ["field_does_not_exist"])
    mg.drop_series(mg.edge.assign1)
    select_data = mg.select_edges()
    is_as_selected(select_data, {}, ["edge.u"], ["edge.assign1", "field_does_not_exist"])
