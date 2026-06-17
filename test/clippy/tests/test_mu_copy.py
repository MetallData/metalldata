import pytest
from clippy import MetallGraph, MetallUtils  # type: ignore
from conftest import DATA_DIR, is_as_described, is_as_selected


def test_mu_copy(tmp_path):
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")

    mg_src = MetallGraph(src)
    mg_src.ingest_parquet_edges(DATA_DIR + "/test", "s", "t")
    is_as_described(mg_src, 21, 28)

    mu = MetallUtils()
    mu.copy(src, dst)

    mg_dst = MetallGraph(dst)
    is_as_described(mg_dst, 21, 28)
    el = mg_dst.select_edges()
    is_as_selected(el, {}, ["edge.u", "edge.v", "edge.graphnum", "edge.relevant"], ["field_does_not_exist"])
