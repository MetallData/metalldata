# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import Any
import pytest
import shutil
import clippy
from clippy import MetallGraph, MetallUtils  # type: ignore
import os
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


def is_as_selected(
    data_list: list[dict],
    required_pairs: dict,
    required_fields: list[str],
    missing_fields: list[str],
):
    for el in data_list:
        assert required_pairs.items() <= el.items()
        for missing in missing_fields:
            assert missing not in el.keys()
        for required in required_fields:
            assert required in el.keys()


def is_specific(
    data_list: list[dict[str, Any]], key: str, keydict: dict[str, dict[str, Any]]
):
    for el in data_list:
        if key in el:
            keyval = el[key]
            if keyval in keydict:
                assert (
                    keydict[keyval].items() <= el.items()
                    and len(keydict[keyval].items()) > 0
                )


@pytest.fixture(scope="session")
def reference_graph_path(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("reference") / "metallgraph.db")
    mg = MetallGraph(path)
    mg.ingest_parquet_edges(DATA_DIR + "/test", "s", "t")
    mg.assign("node.gnum", 3, where=mg.edge.graphnum == 3)
    return path


@pytest.fixture()
def metallgraph(reference_graph_path, tmp_path):
    dst = str(tmp_path / "metallgraph.db")
    MetallUtils().copy(reference_graph_path, dst)    
    return MetallGraph(dst)

