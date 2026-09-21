# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Main orchestrator for the Data Quality Framework."""

import logging
import time
from collections import defaultdict
from urllib.parse import urlparse

from pyhocon import ConfigFactory

from dq.engine.engine_loader import EngineLoader
from dq.utils import config_utils, constants
from dq.catalog.catalog_factory import CatalogFactory
from dq.exceptions import ConfigurationError, DataFrameNotFoundError, ValidationError
from dq.identifiers import IdentifierError, TableName
from dq.outcomes import MAX_BATCH_BYTES, MAX_OUTCOMES, CheckOutcome, normalize_outcomes
from dq.sinks import sink_from_config

logger = logging.getLogger(__name__)


class DQFramework:
    """Main orchestrator that loads configurations, manages DataFrames,
    and coordinates engine execution for data quality validation.

    Args:
        spark: Active SparkSession.
        config: Configuration string (HOCON), or URI (file://, s3://, abfss://, http://).
        default_dataframe: Optional default DataFrame for rules that don't
            specify explicit dataframe references.

    Configuration supports a ``catalog_type`` key to control how table
    references are resolved:

    - ``"spark"`` (default): Uses Spark's built-in catalog.
    - ``"unity"``: Uses Databricks Unity Catalog (three-part names).
    - ``"glue"``: Uses AWS Glue Data Catalog.
    - ``"hive"``: Uses Hive metastore via Spark.

    Example::

        dqframework {
          catalog_type = "unity"

          dataframes {
            orders = "main.sales.orders"
            customers = "main.sales.customers"
          }

          dqrules = [...]
        }

    Or with explicit catalog components::

        dqframework {
          catalog_type = "glue"

          dataframes {
            orders {
              database = "sales_db"
              table = "orders"
            }
          }

          dqrules = [...]
        }
    """

    def __init__(self, spark, config, default_dataframe=None):
        self._spark = spark
        self._config = self._load_config(config)
        self.default_dataframe = default_dataframe
        self._catalog_type = self._config.get("dqframework.catalog_type", None)
        self._catalog_provider = CatalogFactory.get_provider(
            spark, catalog_type=self._catalog_type
        )
        self.dataframes = self._load_dataframes()

    def _load_config(self, config):
        """Load HOCON configuration from various sources.

        Args:
            config: Config string or URI.

        Returns:
            Parsed ConfigTree.

        Raises:
            ConfigurationError: If config cannot be parsed.
        """
        parsed_url = urlparse(config)
        try:
            if parsed_url.scheme == "":
                return ConfigFactory.parse_string(config)
            elif parsed_url.scheme == "s3":
                return ConfigFactory.parse_string(
                    config_utils.load_from_s3(
                        bucket=parsed_url.netloc, key=parsed_url.path
                    )
                )
            elif parsed_url.scheme == "abfss":
                return ConfigFactory.parse_string(config_utils.load_from_adls(config))
            elif parsed_url.scheme == "file":
                return ConfigFactory.parse_file(config.replace("file://", ""))
            elif parsed_url.scheme in ["http", "https"]:
                return ConfigFactory.parse_string(config_utils.load_from_uri(config))
            else:
                raise ConfigurationError(
                    f"Unsupported config scheme: {parsed_url.scheme}"
                )
        except ConfigurationError:
            raise
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {e}") from e

    def _load_dataframes(self):
        """Load DataFrames from configuration.

        Supports both simple string references (table names) and
        structured references with catalog/database/table components.

        Returns:
            Dict mapping logical names to DataFrames.
        """
        dataframes = {}
        config_dataframes = self._config.get("dqframework.dataframes", {})

        for df_name, table_ref in config_dataframes.items():
            # An explicitly injected default is already the authoritative source for
            # the logical "default" dataset. Avoid resolving a shadowed catalog
            # reference (commonly a placeholder in reusable rule configurations).
            if df_name == "default" and self.default_dataframe is not None:
                dataframes[df_name] = self.default_dataframe
                continue
            try:
                df = self._resolve_dataframe(df_name, table_ref)
                if df is not None:
                    dataframes[df_name] = df
            except Exception as e:
                raise DataFrameNotFoundError(
                    f"Could not load configured DataFrame '{df_name}': {e}"
                ) from e

        if self.default_dataframe is not None and "default" not in dataframes:
            dataframes["default"] = self.default_dataframe

        return dataframes

    def _resolve_dataframe(self, df_name, table_ref):
        """Resolve a DataFrame reference using the configured catalog.

        Handles both string references ("catalog.schema.table") and
        structured config references with database/table/catalog keys.

        Args:
            df_name: Logical name for the DataFrame.
            table_ref: String table name or ConfigTree with components.

        Returns:
            Spark DataFrame or None.
        """
        if isinstance(table_ref, str):
            # Simple string reference: could be "table", "db.table", or "catalog.db.table"
            return self._catalog_provider.get_dataframe(table_ref)
        elif hasattr(table_ref, "get"):
            # Structured reference with explicit components
            table = table_ref.get("table", df_name)
            database = table_ref.get("database", None)
            catalog = table_ref.get("catalog", None)
            return self._catalog_provider.get_dataframe(
                table, database=database, catalog=catalog
            )
        else:
            # Fallback: treat as string
            return self._catalog_provider.get_dataframe(str(table_ref))

    def run(self):
        """Execute all configured data quality rules.

        Returns:
            List of metric dictionaries with keys:
                - ``check``: Check description
                - ``success``: Boolean result
                - ``details``: Detailed check output
                - ``ts``: Timestamp in milliseconds
                - ``jobid``: Spark application ID
        """
        from contextlib import nullcontext

        current_time_in_millis = time.time_ns() // 1_000_000
        cumulative_metrics = []
        cumulative_bytes = 0
        sink = sink_from_config(self._config.get("dqframework.repository", {}))

        rule_configs = self._config.get("dqframework.dqrules", [])
        if not rule_configs:
            raise ConfigurationError("No dqrules defined in configuration")

        scope = (
            sink.run_scope(current_time_in_millis)
            if sink is not None
            else nullcontext()
        )
        with scope:
            for rule_index, rule_config in enumerate(rule_configs):
                df_names = rule_config.get("dataframes", ["default"])
                engine_name = rule_config.get(constants.DQ_ENGINE_NAME, None)

                if engine_name is None:
                    raise ConfigurationError(
                        f"Rule {rule_index} is missing required key 'engine'"
                    )

                engine = EngineLoader().load_engine(
                    engine_name, rule_config, current_time_in_millis
                )

                for df_name in df_names:
                    dataframe = self.get_dataframe(df_name)
                    summary_metrics = engine.apply(dataframe, repository=sink)
                    for outcome in normalize_outcomes(summary_metrics):
                        metric = outcome.to_legacy()
                        metric["ts"] = current_time_in_millis
                        metric["jobid"] = self._spark.sparkContext.applicationId
                        enriched = CheckOutcome.from_legacy(metric)
                        cumulative_bytes += enriched.serialized_size
                        if (
                            len(cumulative_metrics) >= MAX_OUTCOMES
                            or cumulative_bytes > MAX_BATCH_BYTES
                        ):
                            raise ValidationError(
                                "Execution outcomes exceed summary limits"
                            )
                        if not outcome.success:
                            logger.warning(
                                "Check failed: %s", metric.get("check", "unnamed")
                            )
                        cumulative_metrics.append(metric)

            return cumulative_metrics

        return cumulative_metrics

    def get_dataframe(self, df_name):
        """Retrieve a DataFrame by logical name.

        Checks in order:
        1. Default DataFrame (if df_name is "default")
        2. Pre-loaded DataFrames from config
        3. Spark catalog (temp views / tables)
        4. Configured catalog provider

        Args:
            df_name: Logical DataFrame name.

        Returns:
            Spark DataFrame.

        Raises:
            DataFrameNotFoundError: If DataFrame cannot be found.
        """
        if df_name == "default" and self.default_dataframe is not None:
            return self.default_dataframe
        elif df_name in self.dataframes:
            return self.dataframes[df_name]
        elif df_name in [t.name for t in self._spark.catalog.listTables()]:
            try:
                TableName.parse(df_name)
            except IdentifierError as error:
                raise DataFrameNotFoundError(
                    f"Invalid table name format: '{df_name}'"
                ) from error
            return self._spark.table(df_name)
        else:
            # Try the catalog provider as last resort
            try:
                return self._catalog_provider.get_dataframe(df_name)
            except Exception:
                raise DataFrameNotFoundError(
                    f"DataFrame '{df_name}' not found in config, Spark catalog, "
                    f"or {self._catalog_type or 'default'} catalog."
                )

    def pivot_configuration(self, config):
        """Pivot configuration so all engines and their checks are grouped.

        Args:
            config: Configuration object with rules attribute.

        Returns:
            Dict mapping engine names to lists of checks.
        """
        engine_check_map = defaultdict(list)
        for rule in config.rules:
            engine_check_map[rule.engine].extend(rule.checks)
        return engine_check_map
