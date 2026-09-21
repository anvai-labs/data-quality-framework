# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional, List, Dict, Any

from pyhocon import ConfigTree
from pyspark.sql import DataFrame
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.repository import FileSystemMetricsRepository, ResultKey

from dq.engine.dq_engine import DQEngine
from dq.engine.deequ.deequ_check import DeequCheck
from dq.utils import constants

logger = logging.getLogger(__name__)


class DeequEngine(DQEngine):
    """Engine that validates DataFrames using Amazon Deequ.

    Wraps PyDeequ's ``VerificationSuite`` to run constraint checks
    defined in the HOCON configuration.  Supports both single-check
    mode (all constraints in one ``Check``) and multi-check mode
    (one ``Check`` per constraint).
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._sparkSession = None
        super().__init__(config, dqts)

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Run Deequ verification checks against the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config for persisting metrics.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        rule_name = self._config.get(constants.DQ_RULE_NAME, "Unknown")
        engine_name = self._config.get(constants.DQ_ENGINE_NAME, "Unknown")
        logger.info("Processing %s with %s Engine", rule_name, engine_name)

        deequ_check = DeequCheck(
            checks_config=self._config.get("checks", []),
            single_check_mode=self._config.get(constants.DQ_SINGLE_CHECK_MODE, False),
        )
        self._sparkSession = dataframe.sparkSession

        verification_run_builder = VerificationSuite(
            spark_session=self._sparkSession
        ).onData(df=dataframe)

        verification_run_builder = deequ_check.apply_checks(
            verification_run_builder, self._sparkSession
        )

        verification_result = verification_run_builder.run()

        if verification_result.status == "SUCCESS":
            logger.info("Data quality checks passed successfully.")
        else:
            logger.warning("Data quality checks failed.")

        successMetrics = VerificationResult.successMetricsAsDataFrame(
            spark_session=self._sparkSession,
            verificationResult=verification_result,
        )
        checkVerifications = VerificationResult.checkResultsAsDataFrame(
            spark_session=self._sparkSession,
            verificationResult=verification_result,
        )

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
