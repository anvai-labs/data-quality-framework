# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import logging
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationResult, VerificationSuite
from pydeequ.suggestions import *
from pyspark.sql import functions as F, SparkSession

from dq.engine.deequ.expression import apply_constraint_suggestion

logger = logging.getLogger(__name__)


class DeequConstraintsBuilder:
    """Generates and runs Deequ constraint suggestions for a DataFrame."""

    @staticmethod
    def apply_suggestions(check, constraint_suggestions):
        """Apply generated suggestions through the restricted DSL adapter."""
        for suggestion in constraint_suggestions:
            try:
                expression = suggestion["code_for_constraint"]
            except (KeyError, TypeError) as error:
                raise ValueError(
                    "Constraint suggestion is missing code_for_constraint"
                ) from error
            check = apply_constraint_suggestion(check, expression)
        return check

    def build_constraints(self, spark, df):
        """Generate suggested data quality constraints for a DataFrame.

        Args:
            spark: Active SparkSession.
            df: Spark DataFrame to analyse.

        Returns:
            Dictionary of constraint suggestions from Deequ.
        """
        suggested_constraints = (
            ConstraintSuggestionRunner(spark)
            .onData(df)
            .addConstraintRule(CompleteIfCompleteRule())
            .addConstraintRule(NonNegativeNumbersRule())
            .addConstraintRule(RetainCompletenessRule())
            .addConstraintRule(RetainTypeRule())
            .addConstraintRule(UniqueIfApproximatelyUniqueRule())
            .run()
        )

        return suggested_constraints

    def save_in_hocon_format(
        self, constraint_suggestions, dataset_name, dq_metrics_table, file_name
    ):
        """Save constraint suggestions as a HOCON configuration file.

        Args:
            constraint_suggestions: Output from ``build_constraints``.
            dataset_name: Name of the dataset for config metadata.
            dq_metrics_table: Metrics table reference for repository config.
            file_name: File path to write the HOCON config to.

        Returns:
            The file path that was written.
        """
        transform_suggestion = []
        for cons in constraint_suggestions["constraint_suggestions"]:
            s = cons["code_for_constraint"]
            constraint = s[1 : s.index("(")]
            assertion = ""
            if "lambda" in s:
                assertion = s[s.index("lambda") : s.index(",", s.index("lambda"))]
            datatype = ""
            if "hasDataType" in s:
                datatype = str(
                    cons["current_value"][cons["current_value"].index(":") + 1 :]
                ).strip()
            check_str = f"""{{
                                alias = "{cons['constraint_name']}"
                                column = "{cons['column_name']}"
                                level = "Warning"
                                constraint = "{constraint}"
                                assertion = "{assertion}"
                                datatype = "{datatype}"
                            }},"""
            transform_suggestion.append(check_str)
        transform_suggestion = "".join(transform_suggestion).replace("'", "")
        dq_conf = f"""
            dqframework {{
            requiredscore = 1.0

            repository {{
              dataset ="{dataset_name}"
              format = "json"
              catalog {{
              tables = [ {dq_metrics_table}]
              }}
            }}
            dqrules = [
              {{
                name = "{dataset_name}-rules"
                engine = "deequ"
                checks = [
                    {transform_suggestion}
                    ]
              }}
            ]
          }}
        """

        parent_directory = os.path.dirname(file_name)
        if parent_directory:
            os.makedirs(parent_directory, exist_ok=True)

        with open(file_name, "w") as conf_file:
            conf_file.write(dq_conf)
        logger.info("Config file saved to: %s", file_name)
        return file_name

    def run_dqrules_for_dataset(self, spark, df, constraints):
        """Run Deequ validation rules built from constraint suggestions.

        Args:
            spark: Active SparkSession.
            df: Spark DataFrame to validate.
            constraints: Constraint suggestions from ``build_constraints``.

        Returns:
            DataFrame of check results.
        """
        for suggestion in constraints["constraint_suggestions"]:
            logger.debug(
                "Suggested constraint for '%s': %s",
                suggestion["column_name"],
                suggestion["description"],
            )
            logger.debug("Rule description: '%s'", suggestion["rule_description"])
            logger.debug("Python code: `%s`", suggestion["code_for_constraint"])

        check = Check(
            spark_session=spark,
            level=CheckLevel.Warning,
            description="Data Quality Check",
        )

        check = self.apply_suggestions(check, constraints["constraint_suggestions"])

        checked_constraints = VerificationSuite(spark).onData(df).addCheck(check).run()

        df_checked_constraints = VerificationResult.checkResultsAsDataFrame(
            spark, checked_constraints
        )

        logger.info(
            df_checked_constraints.show(
                n=df_checked_constraints.count(), truncate=False
            )
        )

        df_checked_constraints_failures = df_checked_constraints.filter(
            F.col("constraint_status") == "Failure"
        )

        if df_checked_constraints_failures.count() > 0:
            logger.info(
                df_checked_constraints_failures.show(
                    n=df_checked_constraints_failures.count(), truncate=False
                )
            )

        return df_checked_constraints
