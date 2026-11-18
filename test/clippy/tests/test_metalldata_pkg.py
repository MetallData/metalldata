# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
import subprocess
import sys

import pytest
import metalldata
from metalldata import MetallGraph, MetallUtils  # type: ignore
from metalldata import errors


@pytest.fixture()
def attached():
    """Attach names to MetallGraph via metalldata._attach, removing them afterward
    so tests don't leak attributes onto the shared class."""
    names = []

    def attach(name, obj):
        metalldata._attach(MetallGraph, name, obj)
        names.append(name)

    yield attach
    for name in names:
        delattr(MetallGraph, name)


def test_import_without_backend_path_env():
    """The package finds the backend on its own; CLIPPY_BACKEND_PATH isn't needed,
    and the top-level (pip) clippy package is never imported."""
    env = {k: v for k, v in os.environ.items() if k != "CLIPPY_BACKEND_PATH"}
    code = (
        "import sys\n"
        "from metalldata import MetallGraph, MetallUtils\n"
        "assert hasattr(MetallGraph, 'describe')\n"
        "assert hasattr(MetallUtils, 'copy')\n"
        "assert 'clippy' not in sys.modules\n"
    )
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_exports():
    assert set(metalldata.__all__) == {"MetallGraph", "MetallUtils", "errors", "logger"}
    assert MetallUtils.__name__ == "MetallUtils"
    assert issubclass(errors.ClippyValidationError, errors.ClippyError)
    assert issubclass(errors.NonZeroReturnCodeError, Exception)


def test_attach_rejects_backend_method():
    with pytest.raises(AttributeError, match="already defines 'describe'"):
        metalldata._attach(MetallGraph, "describe", lambda self: None)


@pytest.mark.parametrize("name", ["node", "edge"])
def test_attach_rejects_keypath(name):
    with pytest.raises(AttributeError, match=f"already defines '{name}'"):
        metalldata._attach(MetallGraph, name, None)


def test_attach_rejects_existing_attachment(attached):
    attached("_pkgtest_value", 1)
    with pytest.raises(AttributeError, match="already defines '_pkgtest_value'"):
        metalldata._attach(MetallGraph, "_pkgtest_value", 2)
    assert MetallGraph._pkgtest_value == 1


def test_attached_method_works_on_instance(attached, metallgraph):
    """A Python method attached to the class is callable on backend-created
    instances and can use backend methods."""

    def node_count(self):
        return self.describe()["nv"]

    attached("_pkgtest_node_count", node_count)
    assert metallgraph._pkgtest_node_count() == 21
