# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""Python interface to MetallData.

    from metalldata import MetallGraph, MetallUtils

MetallGraph and MetallUtils are built from the clippy backend executables
when this package is imported. Python-side additions are attached directly to
those classes (see _attach). Import them from here, not from `clippy`: a
class built by a separate clippy import is a different object and will not
have the additions.
"""

from typing import Any

from . import errors
from ._clippy import logger
from ._clippy.backends import fs as _fs
from ._paths import BACKEND_DIR as _BACKEND_DIR

try:
    _classes = _fs.classes(paths=[_BACKEND_DIR])
    MetallGraph = _classes["MetallGraph"]
    MetallUtils = _classes["MetallUtils"]
except (FileNotFoundError, KeyError) as e:
    raise ImportError(
        f"metalldata backend not found in {_BACKEND_DIR} ({e!r}); has metalldata been built?"
    ) from e


def _attach(cls: type, name: str, obj: Any) -> None:
    """Add `obj` to `cls` as `name`, refusing to shadow a backend method or keypath."""
    if hasattr(cls, name) or name in getattr(cls, "_initial_keypaths", {}):
        raise AttributeError(f"{cls.__name__} already defines {name!r}; refusing to overwrite it")
    setattr(cls, name, obj)


__all__ = ["MetallGraph", "MetallUtils", "errors", "logger"]
