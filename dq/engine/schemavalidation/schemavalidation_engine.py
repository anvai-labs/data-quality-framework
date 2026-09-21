# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional, List, Dict, Any

from dq.engine.deequ.deequ_engine import DeequEngine
from pyhocon import ConfigTree
from pyspark.sql import DataFrame
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.repository import ResultKey

from dq.utils import constants
from dq.engine.schemavalidation.schemavalidation_check import SchemavalidationCheck

logger = logging.getLogger(__name__)


class SchemavalidationEngine(DeequEngine):
    """Engine that validates DataFrame schemas against expected definitions.

    Extends ``DeequEngine`` to check datatype, nullable, unique, and
    foreign-key constraints using Spark schema introspection.  Supports
    Unity Catalog, Glue, Hive, and default Spark catalog types.
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Run schema validation checks against the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config for persisting metrics.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        rule_name = self._config.get(constants.DQ_RULE_NAME, "Unknown")
        engine_name = self._config.get(constants.DQ_ENGINE_NAME, "Unknown")
        logger.info("Processing %s with %s Engine", rule_name, engine_name)

        self._sparkSession = dataframe.sparkSession
        successMetrics, checkVerifications = self.validate_dataframe(dataframe)

        if repository:
            current_milli_time = ResultKey.current_milli_time()
            repository.save(
                successMetrics,
                constants.DQ_REPOSITORY_METRICS,
                current_milli_time,
            )
            repository.save(
                checkVerifications,
                constants.DQ_REPOSITORY_VERIFICATIONS,
                current_milli_time,
            )

        summarymetrics = []
        for check in checkVerifications.collect():
            summarymetrics.append(
                {
                    "check": check["check"],
                    "success": check["check_status"] == "Success",
                    "details": check.asDict(recursive=True),
                }
            )
        return summarymetrics

    def validate_dataframe(self, df: DataFrame):
        """Validate the DataFrame against schema and additional constraints.

        Args:
            df: Spark DataFrame to validate.

        Returns:
            Tuple of (successMetrics DataFrame, checkVerifications DataFrame).
        """
        catalog_type = self._config.get(
            f"{constants.SCHEMA_VALIDATION_SCHEMA}.catalog_type", None
        )
        single_check_mode = self._config.get(constants.DQ_SINGLE_CHECK_MODE, True)
        schema_validation_check = SchemavalidationCheck(
            schema_config=self._config.get(constants.SCHEMA_VALIDATION_SCHEMA, {}),
            single_check_mode=single_check_mode,
        )

        checks = schema_validation_check.apply_checks(df)
        joined_df = (
            schema_validation_check.get_joined_dataframe_from_foreign_key_constraints(
                df
            )
        )

        if single_check_mode:
            verification_result = (
                VerificationSuite(self._sparkSession)
                .onData(joined_df)
                .addCheck(checks)
                .run()
            )
        else:
            verification_run_builder = VerificationSuite(self._sparkSession).onData(
                joined_df
            )
            for check in checks:
                verification_run_builder = verification_run_builder.addCheck(check)
            verification_result = verification_run_builder.run()

        if verification_result.status == "Success":
            logger.info("Schema validation succeeded.")
        else:
            logger.warning("Schema validation failed.")

        final_successmetrics_result_df = VerificationResult.successMetricsAsDataFrame(
            self._sparkSession, verification_result
        )
        final_checkverification_result_df = VerificationResult.checkResultsAsDataFrame(
            self._sparkSession, verification_result
        )
        return final_successmetrics_result_df, final_checkverification_result_df
