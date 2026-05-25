import pytest
from clippy import MetallGraph  # type: ignore
from conftest import is_as_described


@pytest.fixture()
def empty_graph(tmp_path):
    return MetallGraph(str(tmp_path / "metallgraph.db"))


def test_mg_create(empty_graph):
    is_as_described(empty_graph, 0, 0)
