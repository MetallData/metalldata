# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest
from clippy import MetallGraph  # type: ignore
from conftest import is_as_described


@pytest.fixture()
def empty_graph(tmp_path):
    return MetallGraph(str(tmp_path / "metallgraph.db"))


def test_mg_create(empty_graph):
    is_as_described(empty_graph, 0, 0)
