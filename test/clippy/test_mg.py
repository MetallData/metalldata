import pytest
import clippy
from clippy import MetallGraph # type: ignore
import os
from clippy.error import ClippyValidationError, ClippyInvalidSelectorError
from clippy.backends.fs.execution import NonZeroReturnCodeError
import logging

DATA_DIR = os.environ.get("DATA_DIR", "../../data/metall_graph")

clippy.logger.setLevel(logging.WARN)
logging.getLogger().setLevel(logging.WARN)

def is_as_described(mg: MetallGraph, nv: int, ne: int, check_for_path: bool = True):
    r = mg.describe()
    assert "nv" in r
    assert "ne" in r
    if check_for_path:
        assert "path" in r
    assert r["nv"] == nv
    assert r["ne"] == ne

def is_as_selected(data_list: list[dict], required_pairs: dict, required_fields: list[str], missing_fields: list[str]):
    for el in data_list:
        assert required_pairs.items() <= el.items()
        for missing in missing_fields:
            assert missing not in el.keys()
        for required in required_fields:
            assert required in el.keys()

@pytest.fixture(scope="module")
def temp_file(tmp_path_factory):
    temp_name = tmp_path_factory.mktemp("data") / "metallgraph.db"
    yield str(temp_name)


@pytest.fixture()
def metallgraph(temp_file):
    print(f"{temp_file=}")
    return MetallGraph(temp_file)

@pytest.mark.order(1)
def test_mg_create(metallgraph):
    is_as_described(metallgraph, 0, 0)

@pytest.mark.order(2)
def test_mg_ingest_parquet(metallgraph):
    metallgraph.ingest_parquet_edges(DATA_DIR + "/pq", "s", "t")
    is_as_described(metallgraph, 14, 22)
    el = metallgraph.select_edges()
    is_as_selected(el, {}, ["u", "v", "graphnum", "relevant"], ["foo", "field_does_not_exist"])

@pytest.mark.order(3)
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

@pytest.mark.order(4)
def test_mg_add_faker(metallgraph):
    metallgraph.add_faker("node.uuid", "uuid4")
    select_data = metallgraph.select_nodes()
    is_as_selected(select_data, {}, ["uuid", "id"], ["field_does_not_exist"])
    metallgraph.add_faker("edge.temp", "double", where=metallgraph.edge.graphnum == 0)
    select_data = metallgraph.select_edges(where=metallgraph.edge.graphnum == 0)
    is_as_selected(select_data, {}, ["u", "v", "temp"], ["field_does_not_exist"])
    select_data = metallgraph.select_edges(where=metallgraph.edge.graphnum != 0)
    is_as_selected(select_data, {}, ["u", "v"], ["temp"])


@pytest.mark.order(5)
def test_mg_drop_series(metallgraph):

    select_data = metallgraph.select_nodes()
    is_as_selected(select_data, {}, ["id", "assign1"], ["field_does_not_exist"])
    metallgraph.drop_series(metallgraph.node.assign1)
    select_data = metallgraph.select_nodes()
    is_as_selected(select_data, {}, ["id"], ["assign1", "field_does_not_exist"])

    select_data = metallgraph.select_edges()
    is_as_selected(select_data, {}, ["u", "assign1"], ["field_does_not_exist"])
    metallgraph.drop_series(metallgraph.edge.assign1)
    select_data = metallgraph.select_edges()
    is_as_selected(select_data, {}, ["u"], ["assign1", "field_does_not_exist"])

