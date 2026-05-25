import pytest
from clippy.backends.fs.execution import NonZeroReturnCodeError  # type: ignore
from conftest import is_as_selected


@pytest.fixture()
def rename_graph(metallgraph):
    metallgraph.sample_edges(k=10, series_name="samp1")
    metallgraph.sample_nodes(k=10, series_name="nsamp1")
    return metallgraph


def test_mg_rename_series(rename_graph):
    mg = rename_graph

    mg.rename_series(mg.node.nsamp1, "testrename1")
    select_data = mg.select_nodes(where=mg.node.testrename1 == True)
    is_as_selected(select_data, {"testrename1": True}, ["id"], [])

    mg.rename_series(mg.node.testrename1, "node.testrename2")
    select_data = mg.select_nodes(where=mg.node.testrename2 == True)
    is_as_selected(select_data, {"testrename2": True}, ["id"], [])

    mg.rename_series(mg.node.testrename2, "nsamp1")
    select_data = mg.select_nodes(where=mg.node.nsamp1 == True)
    is_as_selected(select_data, {"nsamp1": True}, ["id"], [])

    mg.rename_series(mg.edge.samp1, "testrename1")
    select_data = mg.select_edges(where=mg.edge.testrename1 == True)
    is_as_selected(select_data, {"testrename1": True}, ["u", "v"], [])

    mg.rename_series(mg.edge.testrename1, "edge.testrename2")
    select_data = mg.select_edges(where=mg.edge.testrename2 == True)
    is_as_selected(select_data, {"testrename2": True}, ["u", "v"], [])

    mg.rename_series(mg.edge.testrename2, "samp1")
    select_data = mg.select_edges(where=mg.edge.samp1 == True)
    is_as_selected(select_data, {"samp1": True}, ["u", "v"], [])

    with pytest.raises(NonZeroReturnCodeError):
        mg.rename_series(mg.edge.samp1, "edge.u")
        mg.rename_series(mg.node.nsamp1, "node.id")

        mg.rename_series(mg.edge.samp1, "node.testrename")
        mg.rename_series(mg.node.nsamp1, "edge.testrename")
        mg.rename_series(mg.edge.samp1, "samp1")
        mg.rename_series(mg.edge.samp1, "edge.samp1")
        mg.rename_series(mg.node.nsamp1, "nsamp1")
        mg.rename_series(mg.node.nsamp1, "node.nsamp1")
