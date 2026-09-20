# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the lightweight package and supported PyDeequ adapter."""

from importlib.metadata import PackageNotFoundError, version
import importlib
from pathlib import Path
import os
import subprocess
import sys
from io import BytesIO
from unittest.mock import Mock

import dq
import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _run_with_blocked_imports(*blocked_modules: str, statement: str):
    script = f"""
import builtins
original_import = builtins.__import__
blocked = {set(blocked_modules)!r}

def guarded_import(name, *args, **kwargs):
    if name.split('.', 1)[0] in blocked:
        raise ModuleNotFoundError(f'blocked optional dependency: {{name}}')
    return original_import(name, *args, **kwargs)

builtins.__import__ = guarded_import
{statement}
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT)
    return subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def test_core_package_imports_without_heavy_optional_dependencies():
    result = _run_with_blocked_imports(
        "boto3",
        "databricks",
        "pydeequ",
        "pyspark",
        statement="import dq; print(dq.__version__)",
    )
    assert result.returncode == 0, result.stderr


def test_config_helpers_import_without_aws_extra():
    result = _run_with_blocked_imports(
        "boto3", statement="from dq.utils import config_utils"
    )
    assert result.returncode == 0, result.stderr


def test_s3_loader_resolves_optional_sdk_only_when_used(monkeypatch):
    from dq.utils.config_utils import load_from_s3

    resource = Mock()
    resource.Object.return_value.get.return_value = {"Body": BytesIO(b"rules")}
    session = Mock()
    session.resource.return_value = resource
    monkeypatch.setattr("boto3.session.Session", lambda: session)
    assert load_from_s3("rules-bucket", "versioned/rules.conf") == "rules"
    session.resource.assert_called_once_with("s3")
    resource.Object.assert_called_once_with("rules-bucket", "versioned/rules.conf")


def test_package_version_has_one_metadata_source():
    try:
        installed_version = version("data-quality-framework")
    except PackageNotFoundError:
        installed_version = "0+unknown"
    assert dq.__version__ == installed_version


def test_framework_facade_remains_available_with_spark_extra():
    from dq import DQFramework
    from dq.dq_framework import DQFramework as ConcreteFramework

    assert DQFramework is ConcreteFramework


def test_unknown_package_attribute_raises():
    with pytest.raises(AttributeError, match="no attribute 'missing'"):
        getattr(dq, "missing")


def test_uninstalled_source_tree_version_is_explicit(monkeypatch):
    def missing_version(name):
        raise PackageNotFoundError(name)

    with monkeypatch.context() as scoped:
        scoped.setattr("importlib.metadata.version", missing_version)
        importlib.reload(dq)
        assert dq.__version__ == "0+unknown"
    importlib.reload(dq)


def test_pydeequ_17_exposes_expected_spark35_adapter_and_dqdl():
    os.environ["SPARK_VERSION"] = "3.5"
    import pydeequ
    from pydeequ.dqdl import EvaluateDataQuality

    assert version("pydeequ") == "1.7.0"
    assert pydeequ.deequ_maven_coord == "com.amazon.deequ:deequ:2.0.21-spark-3.5"
    assert EvaluateDataQuality is not None
