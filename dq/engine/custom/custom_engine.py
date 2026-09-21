# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional, List, Dict, Any

from pydeequ.repository import ResultKey

from dq.engine.dq_engine import DQEngine
from dq.engine.custom.strategies import (
    DistinctnessByGroupStrategy,
    LookupByColumnNameListStrategy,
    RateOfChangeStrategy,
    WideTableNegativeValuesStrategy,
)

from dq.utils import repository_utils, constants

logger = logging.getLogger(__name__)


class CustomEngine(DQEngine):
    """Engine providing custom business-rule constraints.

    Supports four built-in constraint types:

    * ``DistinctnessByGroup`` -- validates distinct counts within groups
    * ``RateOfChange`` -- detects sudden value changes between consecutive rows
    * ``LookupBasedOnColumnNameList`` -- checks column names against a reference table
    * ``WideTablesNegativeValuesCheck`` -- finds negative values across wide tables

    The constraint set is frozen (ADR-003): rule packs depend on these names
    and on the exact metric/verification shapes, so the engine is a permanent
    first-class adapter rather than a migration stop. Each constraint's Spark
    planning lives in one strategy object; every strategy decides through
    distributed aggregations and the driver only receives bounded check
    summaries.
    """

    _STRATEGIES = {
        "DistinctnessByGroup": DistinctnessByGroupStrategy(),
        "RateOfChange": RateOfChangeStrategy(),
        "LookupBasedOnColumnNameList": LookupByColumnNameListStrategy(),
        "WideTablesNegativeValuesCheck": WideTableNegativeValuesStrategy(),
    }

    def __init__(self, config, dqts: Optional[int] = None):
        self._config = config
        super().__init__(config, dqts)

    def apply(self, dataframe, repository=None) -> List[Dict[str, Any]]:
        """Apply custom constraint checks to the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config for persisting metrics.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        custom_checks = self._config.get("checks", {})
        self._sparkSession = dataframe.sparkSession
        _metrics_results = []
        _verification_results = []
        for check_config in custom_checks:
            constraint = check_config.get("constraint", None)
            strategy = self._STRATEGIES.get(constraint)
            if strategy is None:
                raise ValueError(f"Unsupported custom constraint: {constraint}")
            params = {
                "dq_dimension": check_config.get("dq_dimension", "Compliance"),
                "level": check_config.get("level", None),
            }
            if constraint == "DistinctnessByGroup":
                params.update(
                    columns=check_config.get("columns", None),
                    group_by=check_config.get("group_by", None),
                    threshold_min=check_config.get("min", None),
                    threshold_max=check_config.get("max", None),
                )
            elif constraint == "RateOfChange":
                params.update(
                    columns=check_config.get("columns", None),
                    group_by=check_config.get("group_by", None),
                    sort_by=check_config.get("sort_by", None),
                    threshold_min=check_config.get("min", None),
                    threshold_max=check_config.get("max", None),
                )
            elif constraint == "LookupBasedOnColumnNameList":
                params.update(
                    ref_table=check_config.get("ref_table", None),
                    ref_columns=check_config.get("ref_columns", None),
                    ignore_columns=check_config.get("ignore_columns", None),
                    source=check_config.get("source", None),
                )
            elif constraint == "WideTablesNegativeValuesCheck":
                params.update(
                    ignore_columns=check_config.get("ignore_columns", None),
                    source=check_config.get("source", None),
                )
            _results, _check_verification = strategy.apply(dataframe, **params)
            _metrics_results += _results
            _verification_results += _check_verification

        df_metrics_results = self._sparkSession.createDataFrame(
            _metrics_results, ["entity", "instance", "name", "value"]
        )
        df_check_verification_results = self._sparkSession.createDataFrame(
            _verification_results,
            [
                "check",
                "check_level",
                "check_status",
                "constraint",
                "constraint_status",
                "constraint_message",
            ],
        )

        if repository:
            current_milli_time = ResultKey.current_milli_time()
            repository_utils.save_to_repository(
                repository,
                df_metrics_results,
                constants.DQ_REPOSITORY_METRICS,
                current_milli_time,
            )
            repository_utils.save_to_repository(
                repository,
                df_check_verification_results,
                constants.DQ_REPOSITORY_VERIFICATIONS,
                current_milli_time,
            )
        summarymetrics = [
            {
                "check": row[2],
                "success": row[3] == 1,
                "details": {
                    "entity": row[0],
                    "instance": row[1],
                    "name": row[2],
                    "value": row[3],
                },
            }
            for row in _metrics_results
        ]

        return summarymetrics
