import pytest
from clippy import MetallGraph  # type: ignore
from conftest import DATA_DIR, is_as_described, is_as_selected


@pytest.fixture()
def empty_graph(tmp_path):
    return MetallGraph(str(tmp_path / "metallgraph.db"))


def test_mg_ingest_parquet(empty_graph):
    empty_graph.ingest_parquet_edges(DATA_DIR + "/test", "s", "t")
    is_as_described(empty_graph, 21, 28)
    el = empty_graph.select_edges()
    is_as_selected(el, {}, ["edge.u", "edge.v", "edge.graphnum", "edge.relevant"], ["foo", "field_does_not_exist"])
