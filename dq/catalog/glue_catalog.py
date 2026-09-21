# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""AWS Glue Data Catalog provider."""

import logging

from dq.catalog.base import CatalogProvider
from dq.identifiers import TableName, qualified_table_name

logger = logging.getLogger(__name__)


# Mapping from Glue column types to PySpark types
_GLUE_TO_SPARK_TYPE_MAP = {
    "string": "StringType",
    "int": "IntegerType",
    "integer": "IntegerType",
    "bigint": "LongType",
    "long": "LongType",
    "smallint": "ShortType",
    "short": "ShortType",
    "tinyint": "ByteType",
    "byte": "ByteType",
    "float": "FloatType",
    "double": "DoubleType",
    "boolean": "BooleanType",
    "binary": "BinaryType",
    "date": "DateType",
    "timestamp": "TimestampType",
}


def _parse_glue_type(glue_type_str):
    """Convert a Glue type string to a PySpark type instance.

    Handles basic types, decimal(precision,scale), char(n), varchar(n),
    and array/struct/map types by falling back to StringType.

    Args:
        glue_type_str: Glue column type string (e.g., "string", "decimal(10,2)").

    Returns:
        PySpark DataType instance.
    """
    from pyspark.sql.types import (
        StringType,
        IntegerType,
        LongType,
        ShortType,
        ByteType,
        FloatType,
        DoubleType,
        BooleanType,
        BinaryType,
        DateType,
        TimestampType,
        DecimalType,
        CharType,
        VarcharType,
    )
    import re

    glue_lower = glue_type_str.strip().lower()

    # Handle decimal(precision, scale)
    decimal_match = re.match(r"decimal\((\d+),\s*(\d+)\)", glue_lower)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        return DecimalType(precision, scale)

    # Handle char(n) and varchar(n)
    char_match = re.match(r"char\((\d+)\)", glue_lower)
    if char_match:
        return CharType(int(char_match.group(1)))

    varchar_match = re.match(r"varchar\((\d+)\)", glue_lower)
    if varchar_match:
        return VarcharType(int(varchar_match.group(1)))

    # Handle basic types
    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "long": LongType(),
        "smallint": ShortType(),
        "short": ShortType(),
        "tinyint": ByteType(),
        "byte": ByteType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "binary": BinaryType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }

    if glue_lower in type_map:
        return type_map[glue_lower]

    # Fallback for complex types (array, struct, map)
    logger.warning("Unmapped Glue type '%s', falling back to StringType", glue_type_str)
    return StringType()


class GlueCatalogProvider(CatalogProvider):
    """Catalog provider for AWS Glue Data Catalog.

    Supports two modes of operation:

    1. **Spark-through-Glue**: When the SparkSession is configured with
       Glue as the Hive metastore (``spark.hadoop.hive.metastore.client.factory.class``
       = ``com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory``),
       tables are resolved via ``spark.table("database.table")``.

    2. **Direct Glue API**: Uses ``boto3`` Glue client to fetch table metadata
       directly from the Glue Data Catalog. This is used for schema introspection
       when Spark is not configured with Glue as metastore.

    Configuration example::

        dqframework {
          catalog_type = "glue"
          dataframes {
            orders {
              database = "sales_db"
              table = "orders"
            }
          }
        }
    """

    def __init__(self, spark_session, region_name=None, glue_client=None):
        """Initialize the Glue catalog provider.

        Args:
            spark_session: Active SparkSession.
            region_name: AWS region for Glue client. If None, uses default.
            glue_client: Optional pre-configured boto3 Glue client.
        """
        super().__init__(spark_session)
        self._region_name = region_name
        self._glue_client = glue_client

    def _get_glue_client(self):
        """Lazily initialize the Glue client.

        Returns:
            boto3 Glue client.
        """
        if self._glue_client is None:
            try:
                import boto3

                if self._region_name:
                    self._glue_client = boto3.client(
                        "glue", region_name=self._region_name
                    )
                else:
                    self._glue_client = boto3.client("glue")
            except ImportError:
                raise ImportError(
                    "boto3 is required for Glue Catalog support. "
                    "Install with: pip install data-quality-framework[aws]"
                )
        return self._glue_client

    def get_dataframe(self, table_reference, database=None, catalog=None):
        """Load a table as a Spark DataFrame from Glue Catalog.

        Uses Spark SQL to read the table. The SparkSession should be
        configured with Glue as the metastore, or the table should be
        accessible via Spark catalog.

        Args:
            table_reference: Table name or database.table.
            database: Optional Glue database name.
            catalog: Ignored for Glue (single catalog per account/region).

        Returns:
            Spark DataFrame.
        """
        full_name = self._resolve_table_name(table_reference, database)
        logger.info("Loading DataFrame from Glue Catalog: %s", full_name)
        return self._spark.table(full_name)

    def get_table_schema(self, table_reference, database=None, catalog=None):
        """Fetch table schema from Glue Data Catalog.

        Attempts to fetch schema via the Glue API first for accurate
        metadata. Falls back to Spark if the Glue API is unavailable.

        Args:
            table_reference: Table name or database.table.
            database: Optional Glue database name.
            catalog: Ignored for Glue.

        Returns:
            pyspark.sql.types.StructType.
        """
        db_name, table_name = self._split_table_reference(table_reference, database)

        # Try Glue API first for accurate schema
        try:
            return self._get_schema_from_glue_api(db_name, table_name)
        except Exception as e:
            logger.warning(
                "Could not fetch schema from Glue API for %s.%s: %s. "
                "Falling back to Spark catalog.",
                db_name,
                table_name,
                e,
            )
            # Fall back to Spark
            full_name = self._build_full_table_name(table_name, db_name)
            return self._spark.table(full_name).schema

    def table_exists(self, table_reference, database=None, catalog=None):
        """Check if a table exists in Glue Data Catalog.

        Args:
            table_reference: Table name or database.table.
            database: Optional Glue database name.
            catalog: Ignored for Glue.

        Returns:
            bool.
        """
        db_name, table_name = self._split_table_reference(table_reference, database)
        try:
            client = self._get_glue_client()
            client.get_table(DatabaseName=db_name, Name=table_name)
            return True
        except Exception as e:
            logger.debug("Table %s.%s not found in Glue: %s", db_name, table_name, e)
            # Fall back to Spark
            try:
                full_name = self._build_full_table_name(table_name, db_name)
                return self._spark._jsparkSession.catalog().tableExists(full_name)
            except Exception:
                return False

    def get_table_metadata(self, table_reference, database=None):
        """Fetch full table metadata from Glue API.

        Args:
            table_reference: Table name.
            database: Glue database name.

        Returns:
            Dict containing Glue table metadata.
        """
        db_name, table_name = self._split_table_reference(table_reference, database)
        client = self._get_glue_client()
        response = client.get_table(DatabaseName=db_name, Name=table_name)
        return response["Table"]

    def list_databases(self):
        """List all databases in the Glue Data Catalog.

        Returns:
            List of database names.
        """
        client = self._get_glue_client()
        databases = []
        paginator = client.get_paginator("get_databases")
        for page in paginator.paginate():
            for db in page["DatabaseList"]:
                databases.append(db["Name"])
        return databases

    def list_tables(self, database):
        """List all tables in a Glue database.

        Args:
            database: Glue database name.

        Returns:
            List of table names.
        """
        client = self._get_glue_client()
        tables = []
        paginator = client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database):
            for table in page["TableList"]:
                tables.append(table["Name"])
        return tables

    def _get_schema_from_glue_api(self, database, table_name):
        """Fetch and convert Glue table schema to Spark StructType.

        Args:
            database: Glue database name.
            table_name: Glue table name.

        Returns:
            pyspark.sql.types.StructType.
        """
        from pyspark.sql.types import StructType, StructField

        client = self._get_glue_client()
        response = client.get_table(DatabaseName=database, Name=table_name)
        glue_columns = response["Table"]["StorageDescriptor"]["Columns"]

        # Include partition keys as columns
        partition_keys = response["Table"].get("PartitionKeys", [])

        fields = []
        for col in glue_columns + partition_keys:
            col_name = col["Name"]
            col_type = col["Type"]
            comment = col.get("Comment", "")
            spark_type = _parse_glue_type(col_type)
            # Glue does not track nullable per-column; default to True
            fields.append(
                StructField(
                    col_name, spark_type, nullable=True, metadata={"comment": comment}
                )
            )

        logger.debug(
            "Resolved Glue schema for %s.%s: %d columns",
            database,
            table_name,
            len(fields),
        )
        return StructType(fields)

    def _resolve_table_name(self, table_reference, database=None):
        """Resolve table reference to a validated database.table string.

        Args:
            table_reference: Table name or database.table.
            database: Optional database name.

        Returns:
            Fully-qualified database.table string.

        Raises:
            IdentifierError: If the reference or database is malformed.
        """
        return qualified_table_name(
            table_reference, database=database, label="glue table name", max_parts=2
        )

    def _split_table_reference(self, table_reference, database=None):
        """Split a validated table reference into (database, table).

        Args:
            table_reference: Table name or database.table.
            database: Optional database override.

        Returns:
            Tuple of (database_name, table_name).

        Raises:
            IdentifierError: If the reference is malformed.
        """
        table = TableName.parse(table_reference, label="glue table name", max_parts=2)
        if len(table.parts) == 2:
            return table.parts[0], table.parts[1]
        if database:
            return database, table.parts[0]
        return "default", table.parts[0]
