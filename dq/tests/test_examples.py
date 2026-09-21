# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Executable-example contracts (U7): every example script must at least
compile, and the dependency-free portable demonstration must run from a
source checkout without Spark or PyDeequ installed.
"""

import os
import py_compile
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
REPOSITORY_ROOT = EXAMPLES.parent


@pytest.mark.parametrize(
    "script", sorted(EXAMPLES.glob("*.py")), ids=lambda path: path.name
)
def test_example_scripts_compile(script):
    py_compile.compile(str(script), doraise=True)


def test_portable_counts_example_runs_from_source_checkout():
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT)
    result = subprocess.run(
        [sys.executable, "-m", "examples.portable_counts"],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert '"checks"' in result.stdout
    assert '"success"' in result.stdout
