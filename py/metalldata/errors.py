# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""Exception types raised by metalldata objects."""

from ._clippy.backends.fs.execution import NonZeroReturnCodeError
from ._clippy.error import (
    ClippyBackendError,
    ClippyClassInconsistencyError,
    ClippyConfigurationError,
    ClippyError,
    ClippyInvalidKeypathError,
    ClippyTypeError,
    ClippyValidationError,
)

__all__ = [
    "ClippyBackendError",
    "ClippyClassInconsistencyError",
    "ClippyConfigurationError",
    "ClippyError",
    "ClippyInvalidKeypathError",
    "ClippyTypeError",
    "ClippyValidationError",
    "NonZeroReturnCodeError",
]
