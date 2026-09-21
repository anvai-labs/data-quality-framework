# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import unittest
import os
import json

from pyspark.sql import SparkSession

# from deequ_constraints_builder import DeequConstraintsBuilder

import pytest
from unittest.mock import MagicMock

from dq.engine.deequ.deequ_constraints_builder import DeequConstraintsBuilder


def test_apply_suggestions_preserves_declared_order():
    first_check = MagicMock()
    second_check = MagicMock()
    first_check.isComplete.return_value = second_check
    second_check.hasCompleteness.return_value = "final-check"
    suggestions = [
        {"code_for_constraint": '.isComplete("id")'},
        {
            "code_for_constraint": (
                '.hasCompleteness("price", lambda value: value >= 0.99)'
            )
        },
    ]

    result = DeequConstraintsBuilder.apply_suggestions(first_check, suggestions)

    assert result == "final-check"
    first_check.isComplete.assert_called_once_with("id")
    column, assertion = second_check.hasCompleteness.call_args.args
    assert column == "price"
    assert assertion(1.0) is True


def test_apply_suggestions_rejects_missing_generated_code():
    with pytest.raises(ValueError, match="missing code_for_constraint"):
        DeequConstraintsBuilder.apply_suggestions(MagicMock(), [{}])


def test_build_constraints(spark, sample_dataframe):
    # Create a Spark DataFrame
    deequ_constraints_builder = DeequConstraintsBuilder()

    constraints = deequ_constraints_builder.build_constraints(spark, sample_dataframe)
    assert "constraint_suggestions" in constraints
    assert len(constraints["constraint_suggestions"]) > 1
    print(constraints)
    datatype_cons = {
        "constraint_name": "AnalysisBasedConstraint(DataType(load_dt,None),<function1>,Some(<function1>),None)",
        "column_name": "load_dt",
        "current_value": "DataType: Integral",
        "description": "'load_dt' has type Integral",
        "suggesting_rule": "RetainTypeRule()",
        "rule_description": "If we detect a non-string type, we suggest a type constraint",
        "code_for_constraint": '.hasDataType("load_dt", ConstrainableDataTypes.Integral)',
    }
    constraints["constraint_suggestions"].append(datatype_cons)

    file_name = deequ_constraints_builder.save_in_hocon_format(
        constraints, "TestDataset", "TestMetrics", "dist/Testfile.conf"
    )

    assert file_name == "dist/Testfile.conf"

    checked_constraints = deequ_constraints_builder.run_dqrules_for_dataset(
        spark, sample_dataframe, constraints
    )
    assert checked_constraints.count() == len(constraints["constraint_suggestions"])


pytestmark = pytest.mark.spark
