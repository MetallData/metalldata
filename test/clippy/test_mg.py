from typing import Any
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

def is_specific(data_list: list[dict[str, Any]], key: str, keydict: dict[str, dict[str, Any]]):
    for el in data_list:
        if key in el:  # "id"
            keyval = el[key] # "path-a" or similar node value
            if keyval in keydict:
                assert keydict[keyval].items() <= el.items()


    
@pytest.fixture(scope="module")
def temp_file(tmp_path_factory):
    temp_name = tmp_path_factory.mktemp("data") / "metallgraph.db"
    yield str(temp_name)


@pytest.fixture()
def metallgraph(temp_file):
    return MetallGraph(temp_file)

@pytest.mark.order(1)
def test_mg_create(metallgraph):
    is_as_described(metallgraph, 0, 0)

@pytest.mark.order(2)
def test_mg_ingest_parquet(metallgraph):
    metallgraph.ingest_parquet_edges(DATA_DIR + "/pq", "s", "t")
    is_as_described(metallgraph, 21, 28)
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

    # assign node based on edge where
    # used in test nhops
    metallgraph.assign("node.gnum", 3, where=metallgraph.edge.graphnum == 3)
    select_data = metallgraph.select_nodes(where=metallgraph.edge.graphnum == 3)
    is_as_selected(select_data, {"gnum": 3}, [], [])
    select_data = metallgraph.select_nodes(where=metallgraph.edge.graphnum != 3)
    is_as_selected(select_data, {}, [], ["gnum"])


@pytest.mark.order(4)
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
    

@pytest.mark.order(6)
def test_mg_nhops(metallgraph):    
    metallgraph.nhops("nhops", 2, ["path-a", "path-f"])
    select_data = metallgraph.select_nodes()
    required_result = {"path-a": {"nhops": 0},
                       "path-f": {"nhops": 0},
                       "path-b": {"nhops": 1},
                       "path-g": {"nhops": 1},
                       "path-c": {"nhops": 2}
                       }
    
    is_specific(select_data, "id", required_result)
    select_data = metallgraph.select_nodes(where=metallgraph.node.gnum != 3)
    is_as_selected(select_data, {}, ["id"], ["nhops"])


@pytest.mark.order(7)
def test_mg_topk(metallgraph):    

    # test edges with k = 5 and additional data
    results = metallgraph.topk(metallgraph.edge.randint, k=5, addl_series=[metallgraph.edge.name])

    assert (len(results) == 5)
    curr_max = 10000;
    for result in results:
        assert len(result) == 2
        assert result[0] <= curr_max
        curr_max = result[0]

    # test nodes with k > numrows
    results = metallgraph.topk(metallgraph.node.randpct, k=50000)

    assert (len(results) == metallgraph.describe()["nv"])

    curr_max = 10000;
    for result in results:
        assert len(result) == 1
        assert result[0] <= curr_max
        curr_max = result[0]

    # test edges with where clause
    results = metallgraph.topk(metallgraph.edge.randint, where=metallgraph.edge.graphnum == 1)
    rresults = [x[0] for x in results]
    assert (rresults == [92, 84, 76, 57, 33, 19])


@pytest.mark.order(8)
def test_mg_select_sample_edges(metallgraph):
    # test basic sampling with k < total edges
    for _ in range(20):
        sample_data = metallgraph.select_sample_edges(k=10, series_names=[metallgraph.edge.u, metallgraph.edge.v])
        assert len(sample_data) == 10
        is_as_selected(sample_data, {}, ["u", "v"], [])

    # test sampling with where clause
    for _ in range(5):
        sample_data = metallgraph.select_sample_edges(k=5, series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum], where=metallgraph.edge.graphnum == 0)
        assert len(sample_data) == 5
        for el in sample_data:
            assert el["graphnum"] == 0
        is_as_selected(sample_data, {"graphnum": 0}, ["u", "v"], [])

    # test sampling with k > available edges with where clause
    for _ in range(5):
        sample_data = metallgraph.select_sample_edges(k=1000, series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum], where=metallgraph.edge.graphnum == 1)
        total_edges_g1 = len(metallgraph.select_edges(where=metallgraph.edge.graphnum == 1))
        assert len(sample_data) <= total_edges_g1
        is_as_selected(sample_data, {"graphnum": 1}, ["u", "v"], [])

    # test sampling all edges when k >= total edges
    for _ in range(5):
        total_edges = metallgraph.describe()["ne"]
        sample_data = metallgraph.select_sample_edges(k=total_edges + 10)
        assert len(sample_data) <= total_edges

    # test seeds
    for seed in range(5):
        sd1 = metallgraph.select_sample_edges(10, series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum], seed=seed)

        sd2 = metallgraph.select_sample_edges(10, series_names=[metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum], seed=seed)

        sd3 = metallgraph.select_sample_edges(10, [metallgraph.edge.u, metallgraph.edge.v, metallgraph.edge.graphnum], seed=seed+1)

        assert len(sd1) == len(sd2) == len(sd3)
        assert sd1 == sd2
        assert sd2 != sd3


@pytest.mark.order(9)
def test_mg_sample_edges(metallgraph):
    # test column creation
    metallgraph.sample_edges(k=10, series_name="samp1")
    select_data = metallgraph.select_edges(where=metallgraph.edge.samp1 == True)
    is_as_selected(select_data, {"samp1": True}, ["u", "v"], [])
    assert len(select_data) == 10

    metallgraph.sample_edges(k=3, series_name="samp2", where=metallgraph.edge.graphnum == 1)
    select_data = metallgraph.select_edges(where=metallgraph.edge.samp2 == True)
    is_as_selected(select_data, {"samp2": True, "graphnum": 1}, ["u", "v"], [])
    assert len(select_data) == 3


@pytest.mark.order(10)
def test_mg_select_sample_nodes(metallgraph):
    for _ in range(20):
        sample_data = metallgraph.select_sample_nodes(
            k=10, series_names=[metallgraph.node.id]
        )
        assert len(sample_data) == 10
        is_as_selected(sample_data, {}, ["id"], [])

    for _ in range(5):
        sample_data = metallgraph.select_sample_nodes(
            k=5,
            series_names=[metallgraph.node.id, metallgraph.node.gnum],
            where=metallgraph.node.gnum == 3,
        )
        assert len(sample_data) == 5
        for el in sample_data:
            assert el["gnum"] == 3
        is_as_selected(sample_data, {"gnum": 3}, ["id"], [])

    for _ in range(5):
        sample_data = metallgraph.select_sample_nodes(
            k=1000,
            series_names=[metallgraph.node.id, metallgraph.node.gnum],
            where=metallgraph.node.gnum == 3,
        )
        total_nodes_g3 = len(
            metallgraph.select_nodes(where=metallgraph.node.gnum == 3)
        )
        assert len(sample_data) <= total_nodes_g3
        is_as_selected(sample_data, {"gnum": 3}, ["id"], [])

    for _ in range(5):
        total_nodes = metallgraph.describe()["nv"]
        sample_data = metallgraph.select_sample_nodes(k=total_nodes + 10)
        assert len(sample_data) <= total_nodes

    for seed in range(5):
        sd1 = metallgraph.select_sample_nodes(
            10, series_names=[metallgraph.node.id, metallgraph.node.gnum], seed=seed
        )
        sd2 = metallgraph.select_sample_nodes(
            10, series_names=[metallgraph.node.id, metallgraph.node.gnum], seed=seed
        )
        sd3 = metallgraph.select_sample_nodes(
            10,
            series_names=[metallgraph.node.id, metallgraph.node.gnum],
            seed=seed + 1,
        )

        assert len(sd1) == len(sd2) == len(sd3)
        assert sd1 == sd2
        assert sd2 != sd3


@pytest.mark.order(11)
def test_mg_sample_nodes(metallgraph):
    metallgraph.sample_nodes(k=10, series_name="nsamp1")
    select_data = metallgraph.select_nodes(where=metallgraph.node.nsamp1 == True)
    is_as_selected(select_data, {"nsamp1": True}, ["id"], [])
    assert len(select_data) == 10

    metallgraph.sample_nodes(
        k=3, series_name="nsamp2", where=metallgraph.node.gnum == 3
    )
    select_data = metallgraph.select_nodes(where=metallgraph.node.nsamp2 == True)
    is_as_selected(select_data, {"nsamp2": True, "gnum": 3}, ["id"], [])
    assert len(select_data) == 3


@pytest.mark.order(12)
def test_mg_rename_series(metallgraph):
    metallgraph.rename_series(metallgraph.node.nsamp1, "testrename1")
    select_data = metallgraph.select_nodes(where=metallgraph.node.testrename1 == True)
    is_as_selected(select_data, {"testrename1": True}, ["id"], [])

    metallgraph.rename_series(metallgraph.node.testrename1, "node.testrename2")
    select_data = metallgraph.select_nodes(where=metallgraph.node.testrename2 == True)
    is_as_selected(select_data, {"testrename2": True}, ["id"], [])

    metallgraph.rename_series(metallgraph.node.testrename2, "nsamp1")
    select_data = metallgraph.select_nodes(where=metallgraph.node.nsamp1 == True)
    is_as_selected(select_data, {"nsamp1": True}, ["id"], [])

    metallgraph.rename_series(metallgraph.edge.samp1, "testrename1")
    select_data = metallgraph.select_edges(where=metallgraph.edge.testrename1 == True)
    is_as_selected(select_data, {"testrename1": True}, ["u", "v"], [])

    metallgraph.rename_series(metallgraph.edge.testrename1, "edge.testrename2")
    select_data = metallgraph.select_edges(where=metallgraph.edge.testrename2 == True)
    is_as_selected(select_data, {"testrename2": True}, ["u", "v"], [])

    metallgraph.rename_series(metallgraph.edge.testrename2, "samp1")
    select_data = metallgraph.select_edges(where=metallgraph.edge.samp1 == True)
    is_as_selected(select_data, {"samp1": True}, ["u", "v"], [])

    with pytest.raises(NonZeroReturnCodeError):
        metallgraph.rename_series(metallgraph.edge.samp1, "edge.u")
        metallgraph.rename_series(metallgraph.node.nsamp1, "node.id")

        metallgraph.rename_series(metallgraph.edge.samp1, "node.testrename")
        metallgraph.rename_series(metallgraph.node.nsamp1, "edge.testrename")
        metallgraph.rename_series(metallgraph.edge.samp1, "samp1")
        metallgraph.rename_series(metallgraph.edge.samp1, "edge.samp1")
        metallgraph.rename_series(metallgraph.node.nsamp1, "nsamp1")
        metallgraph.rename_series(metallgraph.node.nsamp1, "node.nsamp1")


    

