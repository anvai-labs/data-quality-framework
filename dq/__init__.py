# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Data Quality Framework - configuration-driven data quality for Apache Spark.

Importing :mod:`dq` intentionally does not import Spark, PyDeequ, or cloud SDKs.
The orchestration facade is loaded only when ``DQFramework`` is requested.
"""

from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dq.dq_framework import DQFramework

try:
    __version__ = version("data-quality-framework")
except PackageNotFoundError:  # Source tree before the package is installed.
    __version__ = "0+unknown"

__all__ = ["DQFramework", "__version__"]


def __getattr__(name: str) -> Any:
    """Load the Spark facade without making heavy extras core dependencies."""
    if name == "DQFramework":
        from dq.dq_framework import DQFramework

        return DQFramework
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
