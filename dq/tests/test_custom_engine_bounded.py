# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Bounded-summary contracts for the custom engine (TD-ARCH-4 / U5).

Every constraint must decide its outcome with distributed aggregations and
return at most one metric summary per (constraint, column), never per group
or per dataset row. Expected values are hand-computed from the fixtures.
"""

from json import dumps
from pyhocon import ConfigFactory
import pytest

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from dq.engine.custom.custom_engine import CustomEngine
from dq.engine.custom.strategies import (
    ConsecutivePercentChangeStrategy,
    GroupedDistinctBoundsStrategy,
)
from dq.exceptions import ConfigurationError
from dq.tests.helpers import summaries_by_instance

_GROUPS_DATA = [
    ("east", "a", 1.0),
    ("east", "b", 2.0),
    ("east", "c", 3.0),
    ("west", "a", 9.0),
    ("west", "a", 9.0),
    ("west", "b", 5.0),
    ("north", "a", 7.0),
    ("north", "a", 7.0),
    ("north", "a", 8.0),
]
_GROUPS_SCHEMA = StructType(
    [
        StructField("region", StringType(), True),
        StructField("id", StringType(), True),
        StructField("val", DoubleType(), True),
    ]
)
# Distinct counts per group: east id=3 val=3; west id=2 val=2; north id=1 val=2.

_RATE_DATA = [
    ("g1", 1, 100.0),
    ("g1", 2, 110.0),
    ("g1", 3, 121.0),
    ("g2", 1, 100.0),
    ("g2", 2, 200.0),
    ("g3", 1, 0.0),
    ("g3", 2, 50.0),
    ("g4", 1, None),
    ("g4", 2, 10.0),
]
_RATE_SCHEMA = StructType(
    [
        StructField("grp", StringType(), True),
        StructField("day", IntegerType(), True),
        StructField("v", DoubleType(), True),
    ]
)
# Pairs and |change|%: g1 10,10; g2 100; g3 prev=0 (not evaluable); g4 null prev (skipped).


def distinctness_config(columns, group_by, min_value=None, max_value=None):
    settings = f"columns = {dumps(columns)}\n        group_by = {dumps(group_by)}"
    if min_value is not None:
        settings += f"\n        min = {min_value}"
    if max_value is not None:
        settings += f"\n        max = {max_value}"
    config = f"""
    sync {{
        checks = [
        {{
            constraint_name = "distinct_check"
            constraint = "DistinctnessByGroup"
            {settings}
            level = "Error"
        }}
        ]
    }}
    """
    return ConfigFactory.parse_string(config).get("sync", {})


def rate_of_change_config(columns, group_by, sort_by, min_value=None, max_value=None):
    settings = (
        f"columns = {dumps(columns)}\n        group_by = {dumps(group_by)}"
        f'\n        sort_by = "{sort_by}"'
    )
    if min_value is not None:
        settings += f"\n        min = {min_value}"
    if max_value is not None:
        settings += f"\n        max = {max_value}"
    config = f"""
    sync {{
        checks = [
        {{
            constraint_name = "roc_check"
            constraint = "RateOfChange"
            {settings}
            level = "Error"
        }}
        ]
    }}
    """
    return ConfigFactory.parse_string(config).get("sync", {})


def lookup_config(ref_table, ref_columns="item_id", ignore_columns=None):
    ignored = (
        f"\n        ignore_columns = {dumps(ignore_columns)}" if ignore_columns else ""
    )
    config = f"""
    sync {{
        checks = [
        {{
            constraint_name = "ref_lookup"
            constraint = "LookupBasedOnColumnNameList"
            ref_table = "{ref_table}"
            ref_columns = "{ref_columns}"{ignored}
            source = "timeSeries"
            level = "Warning"
        }}
        ]
    }}
    """
    return ConfigFactory.parse_string(config).get("sync", {})


def test_distinctness_emits_one_bounded_summary_per_column(spark):
    frame = spark.createDataFrame(_GROUPS_DATA, _GROUPS_SCHEMA)
    results = CustomEngine(distinctness_config(["id", "val"], ["region"], 2, 3)).apply(
        frame
    )
    assert len(results) == 2, "one summary per column, not one per group"
    by_instance = summaries_by_instance(results)
    id_summary = next(
        result
        for instance, result in by_instance.items()
        if instance.endswith("for id")
    )
    val_summary = next(
        result
        for instance, result in by_instance.items()
        if instance.endswith("for val")
    )
    assert id_summary["success"] is False, "north has 1 distinct id, below min 2"
    assert val_summary["success"] is True, "val distinct counts {3,3,2} are in [2,3]"


@pytest.mark.parametrize(
    ("min_value", "max_value", "expected"),
    [
        (2, 3, {"id": False, "val": True}),
        (None, 3, {"id": True, "val": True}),
        (4, None, {"id": False, "val": False}),
        (None, None, {"id": True, "val": True}),
    ],
)
def test_distinctness_threshold_decisions_match_per_group_evaluation(
    spark, min_value, max_value, expected
):
    frame = spark.createDataFrame(_GROUPS_DATA, _GROUPS_SCHEMA)
    results = CustomEngine(
        distinctness_config(["id", "val"], ["region"], min_value, max_value)
    ).apply(frame)
    by_instance = summaries_by_instance(results)
    success = {}
    for instance, result in by_instance.items():
        column = instance.rsplit(" ", 1)[-1]
        success[column] = result["success"]
    assert success == expected


def test_distinctness_empty_input_keeps_single_success_summary(spark):
    empty = spark.createDataFrame([], _GROUPS_SCHEMA)
    results = CustomEngine(distinctness_config(["id"], ["region"], 2)).apply(empty)
    assert len(results) == 1
    summary = results[0]
    assert summary["success"] is True, "current legacy behavior on empty input"
    assert summary["details"]["value"] == 1
    assert summary["details"]["instance"] == "DistinctnessByGroup ['region'] for id"


def test_distinctness_many_groups_stay_bounded(spark):
    rows = []
    for group in range(200):
        rows.append((f"g{group}", "a", 1.0))
        rows.append((f"g{group}", "b", 2.0))
    frame = spark.createDataFrame(rows, _GROUPS_SCHEMA)
    results = CustomEngine(distinctness_config(["id", "val"], ["region"], 1, 5)).apply(
        frame
    )
    assert len(results) == 2, "200 groups must not produce 400 summaries"
    assert all(result["success"] for result in results)


def test_rate_of_change_emits_one_bounded_summary_per_column(spark):
    frame = spark.createDataFrame(_RATE_DATA, _RATE_SCHEMA)
    results = CustomEngine(
        rate_of_change_config(["v"], ["grp"], "day", None, 50)
    ).apply(frame)
    assert len(results) == 1
    assert results[0]["success"] is False, "g2 changes 100% against max 50"


def test_rate_of_change_passing_ranges_report_success(spark):
    frame = spark.createDataFrame(_RATE_DATA, _RATE_SCHEMA)
    results = CustomEngine(
        rate_of_change_config(["v"], ["grp"], "day", None, 150)
    ).apply(frame)
    assert len(results) == 1
    assert results[0]["success"] is True


def test_rate_of_change_zero_denominator_does_not_crash(spark):
    frame = spark.createDataFrame(_RATE_DATA, _RATE_SCHEMA)
    metric_results, verifications = ConsecutivePercentChangeStrategy().apply(
        frame, "RateOfChange", "Compliance", "Error", ["v"], ["grp"], "day", None, 150
    )
    assert metric_results[0][3] == 1, "no pair may violate max 150"
    message = verifications[0][5]
    assert "3 evaluated" in message, "g1 twice, g2 once; g3 and g4 are not evaluable"
    assert "2 pairs skipped" in message, "g3 zero baseline and g4 null baseline"


def test_rate_of_change_null_values_do_not_crash(spark):
    frame = spark.createDataFrame(_RATE_DATA, _RATE_SCHEMA)
    results = CustomEngine(rate_of_change_config(["v"], ["grp"], "day", None, 1)).apply(
        frame
    )
    assert len(results) == 1
    assert isinstance(results[0]["success"], bool)


def test_rate_of_change_empty_input_keeps_single_failed_summary(spark):
    empty = spark.createDataFrame([], _RATE_SCHEMA)
    results = CustomEngine(
        rate_of_change_config(["v"], ["grp"], "day", None, 50)
    ).apply(empty)
    assert len(results) == 1
    assert results[0]["success"] is False, "current legacy behavior on empty input"
    assert results[0]["details"]["value"] == 0
    assert results[0]["details"]["instance"] == "RateOfChange ['grp'] for ['v']"


def test_rate_of_change_single_row_groups_produce_no_pairs(spark):
    single = spark.createDataFrame([("s", 1, 5.0)], _RATE_SCHEMA)
    results = CustomEngine(
        rate_of_change_config(["v"], ["grp"], "day", None, 50)
    ).apply(single)
    assert len(results) == 1
    assert results[0]["success"] is False
    assert results[0]["details"]["value"] == 0


def test_rate_of_change_many_rows_stay_bounded(spark):
    rows = []
    for group in range(1000):
        base = 100.0 + (group % 3)
        rows.append((f"g{group}", 0, base))
        rows.append((f"g{group}", 1, base * 1.1))
    frame = spark.createDataFrame(rows, _RATE_SCHEMA)
    results = CustomEngine(
        rate_of_change_config(["v"], ["grp"], "day", None, 50)
    ).apply(frame)
    assert len(results) == 1, "2000 rows must not produce 2000 summaries"


def test_lookup_matches_column_names_against_reference_values(spark):
    spark.createDataFrame(
        [("item_id",), ("qty",), ("extra",)], ["item_id"]
    ).createOrReplaceTempView("lookup_ref_filled")
    frame = spark.createDataFrame(
        [("item_id", 3, "note_value")], ["item_id", "qty", "note"]
    )
    results = CustomEngine(lookup_config("lookup_ref_filled")).apply(frame)
    assert len(results) == 3
    by_instance = summaries_by_instance(results)
    assert by_instance["LookupBasedOnColumnNameList for item_id "]["success"] is True
    assert by_instance["LookupBasedOnColumnNameList for qty "]["success"] is True
    assert by_instance["LookupBasedOnColumnNameList for note "]["success"] is False


def test_lookup_ignores_configured_columns(spark):
    spark.createDataFrame(
        [("item_id",), ("qty",)], ["item_id"]
    ).createOrReplaceTempView("lookup_ref_small")
    frame = spark.createDataFrame(
        [("item_id", 3, "note_value")], ["item_id", "qty", "note"]
    )
    results = CustomEngine(
        lookup_config("lookup_ref_small", ignore_columns=["note"])
    ).apply(frame)
    assert len(results) == 2
    assert all(result["success"] for result in results)


def test_lookup_empty_reference_fails_every_column(spark):
    spark.createDataFrame([], "item_id string").createOrReplaceTempView(
        "lookup_ref_empty"
    )
    frame = spark.createDataFrame([("a", 1)], ["item_id", "qty"])
    results = CustomEngine(lookup_config("lookup_ref_empty")).apply(frame)
    assert len(results) == 2
    assert all(not result["success"] for result in results)


def test_apply_summaries_keep_the_legacy_shape(spark):
    frame = spark.createDataFrame(_GROUPS_DATA, _GROUPS_SCHEMA)
    results = CustomEngine(distinctness_config(["id"], ["region"], 2, 3)).apply(frame)
    assert results
    for result in results:
        assert set(result) == {"check", "success", "details"}
        assert isinstance(result["success"], bool)
        assert set(result["details"]) == {"entity", "instance", "name", "value"}
        assert result["success"] == (result["details"]["value"] == 1)
        assert result["details"]["entity"] == "MultiColumn"


def test_apply_fails_closed_on_unsupported_constraints(spark):
    frame = spark.createDataFrame(_GROUPS_DATA, _GROUPS_SCHEMA)
    config = distinctness_config(["id"], ["region"])
    config["checks"][0]["constraint"] = "MadeUpConstraint"
    with pytest.raises(ValueError, match="MadeUpConstraint"):
        CustomEngine(config).apply(frame)


def test_distinctness_reports_groups_above_the_maximum(spark):
    frame = spark.createDataFrame(_GROUPS_DATA, _GROUPS_SCHEMA)
    metric_results, verifications = GroupedDistinctBoundsStrategy().apply(
        frame, "DistinctnessByGroup", "Compliance", "Error", ["id"], ["region"], None, 1
    )
    assert metric_results[0][3] == 0, "east and west have more than 1 distinct id"
    message = verifications[0][5]
    assert "above the threshold - 1" in message
    assert "2 of 3 groups" in message


def test_canonical_names_run_with_canonical_instance_strings(spark):
    frame = spark.createDataFrame(_GROUPS_DATA, _GROUPS_SCHEMA)
    config = distinctness_config(["id"], ["region"], 2, 3)
    config["checks"][0]["constraint"] = "GroupedDistinctBounds"
    results = CustomEngine(config).apply(frame)
    assert len(results) == 1
    assert results[0]["details"]["instance"].startswith("GroupedDistinctBounds ")
    assert results[0]["success"] is False


def test_canonical_negative_values_and_reference_table_names_run(spark):
    frame = spark.createDataFrame(
        [(1.0, -5.0, "text")], ["positive", "negative", "label"]
    )
    config = ConfigFactory.parse_string("""
        sync { checks = [ {
            constraint_name = "Negative_values"
            constraint = "NoNegativeValues"
            source = "timeSeries"
            level = "Warning"
        } ] }
        """).get("sync", {})
    by_instance = summaries_by_instance(CustomEngine(config).apply(frame))
    assert by_instance["NoNegativeValues for negative "]["success"] is False
    assert by_instance["NoNegativeValues for positive "]["success"] is True

    spark.createDataFrame([("label",)], ["item_id"]).createOrReplaceTempView(
        "ref_tbl_canonical"
    )
    lookup_frame = spark.createDataFrame([("a", 1)], ["item_id", "qty"])
    lookup = lookup_config("ref_tbl_canonical")
    lookup["checks"][0]["constraint"] = "ColumnNamesInReferenceTable"
    results = CustomEngine(lookup).apply(lookup_frame)
    assert results[0]["details"]["instance"].startswith("ColumnNamesInReferenceTable ")


def test_legacy_aliases_keep_their_historical_output(spark, caplog):
    frame = spark.createDataFrame(_GROUPS_DATA, _GROUPS_SCHEMA)
    legacy_config = distinctness_config(["id"], ["region"], 2, 3)
    canonical_config = distinctness_config(["id"], ["region"], 2, 3)
    canonical_config["checks"][0]["constraint"] = "GroupedDistinctBounds"
    with caplog.at_level("WARNING"):
        legacy_results = CustomEngine(legacy_config).apply(frame)
    canonical_results = CustomEngine(canonical_config).apply(frame)
    assert any("renamed to" in record.message for record in caplog.records)
    assert [
        (result["success"], result["details"]["value"]) for result in legacy_results
    ] == [
        (result["success"], result["details"]["value"]) for result in canonical_results
    ]
    assert (
        legacy_results[0]["details"]["instance"]
        == "DistinctnessByGroup ['region'] for id"
    )
    assert (
        canonical_results[0]["details"]["instance"]
        == "GroupedDistinctBounds ['region'] for id"
    )


def test_lookup_rejects_unsafe_reference_table_identifiers(spark):
    frame = spark.createDataFrame([("a", 1)], ["item_id", "qty"])
    config = lookup_config("lookup_ref; drop table sensitive")
    with pytest.raises(ConfigurationError, match="ref_table"):
        CustomEngine(config).apply(frame)


def test_lookup_rejects_non_identifier_reference_columns(spark):
    frame = spark.createDataFrame([("a", 1)], ["item_id", "qty"])
    config = lookup_config("lookup_ref_filled", ref_columns="item_id; drop")
    with pytest.raises(ConfigurationError, match="ref_columns"):
        CustomEngine(config).apply(frame)


def test_lookup_rejects_missing_reference_identifiers(spark):
    frame = spark.createDataFrame([("a", 1)], ["item_id", "qty"])
    config = ConfigFactory.parse_string("""
        sync { checks = [ {
            constraint_name = "ref_lookup"
            constraint = "LookupBasedOnColumnNameList"
            level = "Warning"
        } ] }
        """).get("sync", {})
    with pytest.raises(ConfigurationError, match="ref_table"):
        CustomEngine(config).apply(frame)


def test_negative_values_constraint_reports_failing_and_non_numeric_columns(spark):
    config = ConfigFactory.parse_string("""
        sync { checks = [ {
            constraint_name = "Negative_values"
            constraint = "WideTablesNegativeValuesCheck"
            source = "timeSeries"
            level = "Warning"
        } ] }
        """).get("sync", {})
    frame = spark.createDataFrame(
        [(1.0, -5.0, "text")], ["positive", "negative", "label"]
    )
    results = CustomEngine(config).apply(frame)
    by_instance = summaries_by_instance(results)
    assert by_instance["WideTablesNegativeValuesCheck for positive "]["success"] is True
    assert (
        by_instance["WideTablesNegativeValuesCheck for negative "]["success"] is False
    )
    assert (
        by_instance["WideTablesNegativeValuesCheck for label "]["success"] is True
    ), "string columns cannot hold negative values and count as passing"


pytestmark = pytest.mark.spark
