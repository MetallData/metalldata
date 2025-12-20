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
    metallgraph.ingest_parquet_edges(DATA_DIR + "/pq", "s", "t", col_dir="directed", metadata=["color", "weight", "name"])
    is_as_described(metallgraph, 14, 22)

