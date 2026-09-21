# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Abstract base class for catalog providers."""

from abc import ABC, abstractmethod
import logging

from dq.identifiers import qualified_table_name

logger = logging.getLogger(__name__)


class CatalogProvider(ABC):
    """Base class for all catalog providers.

    A catalog provider resolves table references and fetches schemas
    from a specific metastore (Spark, Unity Catalog, Glue, Hive).
    """

    def __init__(self, spark_session):
        """Initialize the catalog provider.

        Args:
            spark_session: Active SparkSession instance.
        """
        self._spark = spark_session

    @abstractmethod
    def get_dataframe(self, table_reference, database=None, catalog=None):
        """Load a table as a Spark DataFrame.

        Args:
            table_reference: Table name or fully-qualified table reference.
            database: Optional database/schema name.
            catalog: Optional catalog name (for Unity Catalog).

        Returns:
            Spark DataFrame for the requested table.

        Raises:
            DataFrameNotFoundError: If the table cannot be found.
        """
        raise NotImplementedError

    @abstractmethod
    def get_table_schema(self, table_reference, database=None, catalog=None):
        """Fetch the schema (StructType) for a table.

        Args:
            table_reference: Table name or fully-qualified table reference.
            database: Optional database/schema name.
            catalog: Optional catalog name (for Unity Catalog).

        Returns:
            pyspark.sql.types.StructType representing the table schema.

        Raises:
            DataFrameNotFoundError: If the table cannot be found.
        """
        raise NotImplementedError

    @abstractmethod
    def table_exists(self, table_reference, database=None, catalog=None):
        """Check whether a table exists in the catalog.

        Args:
            table_reference: Table name or fully-qualified table reference.
            database: Optional database/schema name.
            catalog: Optional catalog name (for Unity Catalog).

        Returns:
            True if the table exists, False otherwise.
        """
        raise NotImplementedError

    def _build_full_table_name(self, table, database=None, catalog=None):
        """Build a fully-qualified table name from validated components.

        Args:
            table: Table name.
            database: Optional database/schema name.
            catalog: Optional catalog name.

        Returns:
            Fully-qualified table name string.

        Raises:
            IdentifierError: If any component is malformed (TD-ARCH-2/U4).
        """
        return qualified_table_name(table, database=database, catalog=catalog)
