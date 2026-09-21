# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StringType,
    VarcharType,
    CharType,
    IntegerType,
    LongType,
    ShortType,
    FloatType,
    DoubleType,
    BooleanType,
    DecimalType,
    ByteType,
    BinaryType,
    TimestampType,
    DateType,
    TimestampNTZType,
)
import json, re
from dq.identifiers import ColumnName, TableName
from dq.utils import constants


class SchemavalidationCheck:
    """Builds PyDeequ checks from schema definitions.

    Validates datatype, nullable, unique, and foreign-key constraints
    by introspecting the DataFrame schema and optional catalog metadata.
    """

    def __init__(self, schema_config, single_check_mode=True):
        """
        Initialize the schema validation engine.

        Args:
        - single_check_mode: A flag to determine whether all constraints should be run
          in a single check object (True) or each constraint should have its own check object (False).
        - threshold_list_count: The row count threshold to decide whether to use list-based or join-based foreign key validation.
        """
        self._schema_config = schema_config
        self._single_check_mode = single_check_mode
        self._ref_cache = {}
        self._spark_session = None

    def get_ref_cached_data(self, full_tablename, ref_column):
        """
        Fetch the distinct count and reference dataframe , cache the results to potentially avoid redeundant queries.
        Args:
        - ref_db : Reference Database for Foreign key constraint
        - ref_table: Reference Table for Foreign key constraint
        - ref_column : Reference Colun for Foreign key Constraint
        """
        threshold_list_count = self._schema_config.get(
            constants.SCHEMA_VALIDATION_CHECK_FK_THRESHOLD_COUNT_KEY,
            constants.SCHEMA_VALIDATION_CHECK_FK_THRESHOLD_COUNT_VALUE,
        )
        use_list_check = self._schema_config.get(
            constants.SCHEMA_VALIDATION_CHECK_FK_USE_LIST_KEY, True
        )
        cache_key = f"{full_tablename}.{ref_column}"
        if cache_key not in self._ref_cache:
            # Only calculate if cache key is missing
            reference_table = TableName.parse(
                full_tablename, label="reference table name"
            )
            reference_column = ColumnName.parse(ref_column, label="ref_column")
            alias = ColumnName.parse(f"{ref_column}_alias", label="reference alias")
            ref_df = self._spark_session.sql(
                f"SELECT {reference_column.quoted} AS {alias.quoted} "
                f"FROM {reference_table.quoted}"
            )
            distinct_count = ref_df.distinct().count()
            if use_list_check and distinct_count > threshold_list_count:
                use_list_check = False

            self._ref_cache[cache_key] = {
                "ref_df": ref_df,
                constants.SCHEMA_VALIDATION_CHECK_FK_USE_LIST_KEY: use_list_check,
                "distinct_count": distinct_count,
            }
        # Serve the output from cache

        return self._ref_cache[cache_key]

    def get_joined_dataframe_from_foreign_key_constraints(self, df: DataFrame):
        """
        Sequentially apply foregin key constraints to the given Dataframe using left outer join
        Args:
        - df : The input Dataframe to apply foregin key constraints on
        Returns:
        - The joined dataframe after applying left outer joins wherever applicable.
        """

        joined_df = df
        for fk in self._schema_config.get(
            constants.SCHEMA_VALIDATION_FK_CONSTRAINTS, []
        ):
            src_column = fk.get(constants.SCHEMA_VALIDATION_SRC_COLUMN)
            ref_db = fk.get(constants.SCHEMA_VALIDATION_REF_DB, None)
            ref_table = fk.get(constants.SCHEMA_VALIDATION_REF_TABLE)
            ref_column = fk.get(constants.SCHEMA_VALIDATION_REF_COLUMN)
            full_tablename = self._get_fulltable(db=ref_db, tbl=ref_table)
            cached_data = self.get_ref_cached_data(full_tablename, ref_column)

            if not (cached_data[constants.SCHEMA_VALIDATION_CHECK_FK_USE_LIST_KEY]):
                ref_df = cached_data["ref_df"]
                joined_df = joined_df.join(
                    ref_df,
                    joined_df[src_column] == ref_df[f"{ref_column}_alias"],
                    "left_outer",
                )

        return joined_df

    def create_foreign_key_checks(self, check: Check, df: DataFrame):
        """
        Creates foreign key checks based on whether list-based (IN/ContainedIn) OR left outer join check is applied.

        Args:
        - check: The current check object to append constraints to.
        - df: Dataframe after applying left outer joins.

        Returns:
        - The modified check object with foreign key constraints applied.
        """
        for fk in self._schema_config.get(
            constants.SCHEMA_VALIDATION_FK_CONSTRAINTS, []
        ):
            src_column = fk.get(constants.SCHEMA_VALIDATION_SRC_COLUMN)
            ref_db = fk.get(constants.SCHEMA_VALIDATION_REF_DB, None)
            ref_table = fk.get(constants.SCHEMA_VALIDATION_REF_TABLE)
            ref_column = fk.get(constants.SCHEMA_VALIDATION_REF_COLUMN)
            full_tablename = self._get_fulltable(db=ref_db, tbl=ref_table)
            cached_data = self.get_ref_cached_data(full_tablename, ref_column)

            if cached_data[constants.SCHEMA_VALIDATION_CHECK_FK_USE_LIST_KEY]:
                ref_df = cached_data["ref_df"]
                # Keep th following code commented as pydeequ passes all values in string format as expected on scala version of isContainedIn constraint.
                # The following code could be used to uncomment and parse specific data type if required for data type check not natively supported.
                # Keeping this code here as reference for future as it took lot of error and trials to get this right.
                # ref_column_type= [field.dataType for field in ref_df.schema.fields if field.name == f"{ref_column}_alias"][0]
                # print(f"column=[{ref_column}]\t type=[{ref_column_type}]")
                # if isinstance(ref_column_type, IntegerType) or isinstance(ref_column_type, LongType):
                #    ref_values = ref_df.select(f"{ref_column}_alias").distinct().rdd.flatMap(lambda r: [int(r[0])]).collect()
                # elif isinstance(ref_column_type, FloatType) or isinstance(ref_column_type, DoubleType):
                #    ref_values = ref_df.select(f"{ref_column}_alias").distinct().rdd.flatMap(lambda r: [float(r[0])]).collect()
                # elif isinstance(ref_column_type, BooleanType):
                #    ref_values = ref_df.select(f"{ref_column}_alias").distinct().rdd.flatMap(lambda r: [bool(r[0])]).collect()
                # elif isinstance(ref_column_type, StringType) or isinstance(ref_column_type, VarcharType) or isinstance(ref_column_type, CharType) :
                #    ref_values = ref_df.select(f"{ref_column}_alias").distinct().rdd.flatMap(lambda r: [str(r[0])]).collect()
                # else:
                #    ref_values = ref_df.select(f"{ref_column}_alias").distinct().rdd.flatMap(lambda r: r).collect()
                ref_values = (
                    ref_df.select(f"{ref_column}_alias")
                    .distinct()
                    .rdd.flatMap(lambda r: [str(r[0])])
                    .collect()
                )
                check = check.isContainedIn(
                    column=src_column,
                    allowed_values=ref_values,
                    assertion=lambda k: k == 1.0,
                    hint=f"{src_column} must exist in {full_tablename}.{ref_column}",
                )
            else:
                check = check.satisfies(
                    columnCondition=f"{ref_column}_alias IS NOT NULL",
                    constraintName=f"{src_column} refential integrity check against {full_tablename}.{ref_column}",
                    assertion=lambda k: k == 1.0,
                    hint=f"{src_column} must exist in {full_tablename}.{ref_column}",
                )

        return check

    def _apply_catalog_datatype_checks(self, checks, tableSchema):
        """Apply datatype checks for catalog-fetched schemas (Unity, Glue, Hive).

        For non-Spark catalog types, the schema is fetched from the external
        catalog API and may contain types like VarcharType or CharType that
        appear in DDL metadata but not in Spark query plans. This method
        delegates to the Spark datatype check logic since the schema has
        already been converted to a Spark StructType.

        Args:
            checks: Check object (single_check_mode=True) or list of checks.
            tableSchema: pyspark.sql.types.StructType from the catalog.

        Returns:
            Modified checks with all datatype constraints applied.
        """
        return self._apply_spark_datatype_checks(checks, tableSchema)

    def _get_typeofcheck_data_type(self, data_type, parameters):
        lookupvalue = constants.SCHEMA_VALIDATION_TYPEOF_DATATYPE_MAP.get(
            data_type, "unknown"
        )
        if "decimal" == lookupvalue:
            precision, scale = parameters
            return f"{lookupvalue}({precision},{scale})"
        else:
            return lookupvalue

    def _apply_spark_datatype_checks(self, checks, tableSchema):
        """
        For spark based catalog_type perform schema checks for datatypes. This check type is aligned with Spark generated
        query plan datatypes and certain types such as varchar, char may not be used ever as these are mostly used in metadata
        components such as glue, hive or unity.
        Args:
            - input check object for single_check_mode = True OR list of checks for single_check_mode = False value
            - tableSchema obtained from spark dataframe schema
        Output
            Returns finalized modified checks object or list of checks with all constraints applied.
        """
        assertion_lambda = lambda x: x == 1.0
        # Validate schema for each column
        not_null_contraints = self._schema_config.get(
            constants.SCHEMA_VALIDATION_NOT_NULL_COLUMNS_KEY, []
        )
        for field in tableSchema.fields:
            column_name = field.name
            field_data_type = field.dataType
            nullable = field.nullable
            # If table schema suggest field is nullable verify if it has been overriden to be not null column using not_null_constraint in schema validation config.
            if nullable:
                nullable = not (column_name in not_null_contraints)

            hint_expr = f"column[{column_name}] must be datatype[{field_data_type}] AND nullable[{nullable}]"
            # Apply data type checks
            # print(f"{field}=>{hint_expr}")
            data_type, parameters = self._extract_basetype_and_length(
                datatype=str(field_data_type)
            )

            override = next(
                (
                    o
                    for o in self._schema_config.get(
                        constants.SCHEMA_VALIDATION_OVERRIDE_KEY, []
                    )
                    if column_name == o["column"]
                ),
                None,
            )

            if self._single_check_mode:
                if data_type in constants.SCHEVA_VALIDATION_PYDEEQU_DATATYPE_MAP:
                    # Apply standard datatype checks directly supported within PyDeequ hasDataType constraint using  ConstrainableDataTypes
                    if override:
                        if (
                            constants.SCHEMA_VALIDATION_OVERRIDE_CONFIG_PATTERN_KEY
                            in override
                        ):
                            pattern_regex = override.get(
                                constants.SCHEMA_VALIDATION_OVERRIDE_CONFIG_PATTERN_KEY
                            )
                            checks = checks.hasPattern(
                                column=column_name,
                                pattern=pattern_regex,
                                assertion=assertion_lambda,
                                name=f"override check for column {column_name}",
                                hint=f"{hint_expr}. Pattern={pattern_regex}",
                            )
                            if not (
                                override.get(
                                    constants.SCHEMA_VALIDATION_OVERRIDE_CONFIG_REPLACE_KEY,
                                    True,
                                )
                            ):
                                # This means that pattern check is applied on top of data type.
                                mapped_constrainable_datatype = constants.SCHEVA_VALIDATION_PYDEEQU_DATATYPE_MAP.get(
                                    data_type
                                )
                                if not nullable:
                                    checks = checks.hasDataType(
                                        column=column_name,
                                        datatype=mapped_constrainable_datatype,
                                        assertion=assertion_lambda,
                                        hint=hint_expr,
                                    )
                                else:
                                    typeofcheck_data_type = (
                                        self._get_typeofcheck_data_type(
                                            data_type, parameters
                                        )
                                    )
                                    checks = checks.satisfies(
                                        columnCondition=f"{column_name} is NULL OR typeof({column_name}) = '{typeofcheck_data_type}'",
                                        constraintName=f"column[{column_name}] constraint for datatype[{data_type}] with nullable[{nullable}]",
                                        assertion=assertion_lambda,
                                        hint=hint_expr,
                                    )
                    else:
                        if not nullable:
                            mapped_constrainable_datatype = (
                                constants.SCHEVA_VALIDATION_PYDEEQU_DATATYPE_MAP.get(
                                    data_type
                                )
                            )
                            checks = checks.hasDataType(
                                column=column_name,
                                datatype=mapped_constrainable_datatype,
                                assertion=assertion_lambda,
                                hint=hint_expr,
                            )
                        else:
                            typeofcheck_data_type = self._get_typeofcheck_data_type(
                                data_type, parameters
                            )
                            checks = checks.satisfies(
                                columnCondition=f"{column_name} is NULL OR typeof({column_name}) = '{typeofcheck_data_type}'",
                                constraintName=f"column[{column_name}] constraint for datatype[{data_type}] with nullable[{nullable}]",
                                assertion=assertion_lambda,
                                hint=hint_expr,
                            )
                elif data_type in constants.SCHEVA_VALIDATION_CASTSPARKSQL_DATATYPE_MAP:
                    cast_sparksql_type = (
                        constants.SCHEVA_VALIDATION_CASTSPARKSQL_DATATYPE_MAP.get(
                            data_type
                        )
                    )
                    cast_datatype_expr = f"{column_name} is NULL OR CAST({column_name} AS {cast_sparksql_type}) IS NOT NULL"
                    if not nullable:
                        cast_datatype_expr = (
                            f"CAST({column_name} AS {cast_sparksql_type}) IS NOT NULL"
                        )
                    checks = checks.satisfies(
                        columnCondition=cast_datatype_expr,
                        constraintName=f"column[{column_name}] constaint for datatype[{cast_sparksql_type}] AND nullable[{nullable}]",
                        assertion=assertion_lambda,
                        hint=hint_expr,
                    )
                # if isinstance(data_type, StringType) or isinstance(data_type, CharType) or isinstance(data_type, VarcharType):
                #    checks =  checks.hasDataType(column = column_name, datatype = ConstrainableDataTypes.String, assertion = assertion_lambda, hint = hint_expr )
                # elif isinstance(data_type, IntegerType) or isinstance(data_type, LongType):
                #    checks =  checks.hasDataType(column = column_name, datatype = ConstrainableDataTypes.Integral, assertion = assertion_lambda, hint = hint_expr)
                # elif isinstance(data_type, FloatType) or isinstance(data_type, DoubleType) :
                #    checks =  checks.hasDataType(column = column_name, datatype = ConstrainableDataTypes.Fractional, assertion = assertion_lambda, hint = hint_expr)
                # elif isinstance(data_type, DecimalType):
                #    checks =  checks.hasDataType(column = column_name, datatype = ConstrainableDataTypes.Numeric, assertion = assertion_lambda, hint = hint_expr)
                # elif isinstance(data_type, BooleanType):
                #    checks =  checks.hasDataType(column = column_name, datatype = ConstrainableDataTypes.Boolean, assertion = assertion_lambda, hint = hint_expr)
                # elif isinstance(data_type, DateType):
                #    checks = checks

                else:
                    raise ValueError(f"Error: Unknown {data_type}")
                if parameters:
                    if data_type in ["CharType", "VarcharType"]:
                        checks = checks.hasMaxLength(
                            column=column_name,
                            assertion=lambda l: l <= parameters[0],
                            hint=hint_expr,
                        )
                    elif data_type == "DecimalType":
                        precision, scale = parameters
                        cast_decimal_expr = f"{column_name} is NULL OR CAST({column_name} AS DECIMAL({precision},{scale})) IS NOT NULL"
                        if not nullable:
                            cast_decimal_expr = f"CAST({column_name} AS DECIMAL({precision},{scale})) IS NOT NULL"
                        checks = checks.satisfies(
                            columnCondition=cast_decimal_expr,
                            constraintName=f"field[{column_name}] constaint for specific[DECIMAL({precision},{scale})] AND nullable[{nullable}]",
                            assertion=assertion_lambda,
                            hint=hint_expr,
                        )
                if not nullable:
                    checks = checks.isComplete(column=column_name)
            else:
                check = Check(
                    spark_session=self._spark_session,
                    level=CheckLevel.Error,
                    description="Schema Validation",
                )
                if data_type in constants.SCHEVA_VALIDATION_PYDEEQU_DATATYPE_MAP:
                    if override:
                        if (
                            constants.SCHEMA_VALIDATION_OVERRIDE_CONFIG_PATTERN_KEY
                            in override
                        ):
                            pattern_regex = override.get(
                                constants.SCHEMA_VALIDATION_OVERRIDE_CONFIG_PATTERN_KEY
                            )
                            check = check.hasPattern(
                                column=column_name,
                                pattern=pattern_regex,
                                assertion=assertion_lambda,
                                name=f"override check for column {column_name}",
                                hint=f"{hint_expr}. Pattern={pattern_regex}",
                            )
                            if not (
                                override.get(
                                    constants.SCHEMA_VALIDATION_OVERRIDE_CONFIG_REPLACE_KEY,
                                    True,
                                )
                            ):
                                if not nullable:
                                    # This means that pattern check is applied on top of data type.
                                    mapped_constrainable_datatype = constants.SCHEVA_VALIDATION_PYDEEQU_DATATYPE_MAP.get(
                                        data_type
                                    )
                                    check = check.hasDataType(
                                        column=column_name,
                                        datatype=mapped_constrainable_datatype,
                                        assertion=assertion_lambda,
                                        hint=hint_expr,
                                    )
                                else:
                                    typeofcheck_data_type = (
                                        self._get_typeofcheck_data_type(
                                            data_type, parameters
                                        )
                                    )
                                    check = check.satisfies(
                                        columnCondition=f"{column_name} is NULL OR typeof({column_name}) = '{typeofcheck_data_type}'",
                                        constraintName=f"column[{column_name}] constraint for datatype[{data_type}] with nullable[{nullable}]",
                                        assertion=assertion_lambda,
                                        hint=hint_expr,
                                    )
                    else:
                        if not nullable:
                            # Apply standard datatype checks directly supported within PyDeequ hasDataType constraint using  ConstrainableDataTypes
                            mapped_constrainable_datatype = (
                                constants.SCHEVA_VALIDATION_PYDEEQU_DATATYPE_MAP.get(
                                    data_type
                                )
                            )
                            check = check.hasDataType(
                                column=column_name,
                                datatype=mapped_constrainable_datatype,
                                assertion=assertion_lambda,
                                hint=hint_expr,
                            )
                        else:
                            typeofcheck_data_type = self._get_typeofcheck_data_type(
                                data_type, parameters
                            )
                            check = check.satisfies(
                                columnCondition=f"{column_name} is NULL OR typeof({column_name}) = '{typeofcheck_data_type}'",
                                constraintName=f"column[{column_name}] constraint for datatype[{data_type}] with nullable[{nullable}]",
                                assertion=assertion_lambda,
                                hint=hint_expr,
                            )
                elif data_type in constants.SCHEVA_VALIDATION_CASTSPARKSQL_DATATYPE_MAP:
                    cast_sparksql_type = (
                        constants.SCHEVA_VALIDATION_CASTSPARKSQL_DATATYPE_MAP.get(
                            data_type
                        )
                    )
                    cast_datatype_expr = f"{column_name} is NULL OR CAST({column_name} AS {cast_sparksql_type}) IS NOT NULL"
                    if not nullable:
                        cast_datatype_expr = (
                            f"CAST({column_name} AS {cast_sparksql_type}) IS NOT NULL"
                        )
                    check = check.satisfies(
                        columnCondition=cast_datatype_expr,
                        constraintName=f"column[{column_name}] constaint for datatype[{cast_sparksql_type}] AND nullable[{nullable}]",
                        assertion=assertion_lambda,
                        hint=hint_expr,
                    )
                # if isinstance(data_type, StringType) or isinstance(data_type, CharType) or isinstance(data_type, VarcharType):
                #    check =  Check(spark_session = spark_session, level = CheckLevel.Error, description= "Schema Validation").hasDataType(column = column_name, datatype = ConstrainableDataTypes.String, assertion = assertion_lambda, hint = hint_expr)
                # elif isinstance(data_type, IntegerType) or isinstance(data_type, LongType):
                #    check =  Check(spark_session = spark_session, level = CheckLevel.Error, description= "Schema Validation").hasDataType(column = column_name, datatype = ConstrainableDataTypes.Integral, assertion = assertion_lambda, hint = hint_expr)
                # elif isinstance(data_type, FloatType):
                #    check =  Check(spark_session = spark_session, level = CheckLevel.Error, description= "Schema Validation").hasDataType(column = column_name, datatype = ConstrainableDataTypes.Fractional, assertion = assertion_lambda, hint = hint_expr)
                # elif isinstance(data_type, DecimalType):
                #    check =  Check(spark_session = spark_session, level = CheckLevel.Error, description= "Schema Validation").hasDataType(column = column_name, datatype = ConstrainableDataTypes.Numeric, assertion = assertion_lambda, hint = hint_expr)
                # elif isinstance(data_type, BooleanType):
                #    check =  Check(spark_session = spark_session, level = CheckLevel.Error, description= "Schema Validation").hasDataType(column = column_name, datatype = ConstrainableDataTypes.Boolean, assertion = assertion_lambda, hint = hint_expr)
                else:
                    raise ValueError(f"Error: Unknown {data_type}")
                checks.append(check)
                if parameters:
                    if data_type in ["CharType", "VarcharType"]:
                        checks.append(
                            Check(
                                spark_session=self._spark_session,
                                level=CheckLevel.Error,
                                description="Schema Validation Max Length Check",
                            ).hasMaxLength(
                                column=column_name,
                                assertion=lambda l: l <= parameters[0],
                                hint=hint_expr,
                            )
                        )
                    elif data_type == "DecimalType":
                        precision, scale = parameters
                        cast_decimal_expr = f"{column_name} is NULL OR CAST({column_name} AS DECIMAL({precision},{scale})) IS NOT NULL"
                        if not nullable:
                            cast_decimal_expr = f"CAST({column_name} AS DECIMAL({precision},{scale})) IS NOT NULL"
                        checks.append(
                            Check(
                                spark_session=self._spark_session,
                                level=CheckLevel.Error,
                                description="Schema Validation Decimal Type Precision, Scale Check",
                            ).satisfies(
                                columnCondition=cast_decimal_expr,
                                constraintName=f"field[{column_name}] constaint for specific[DECIMAL({precision},{scale})] AND nullable[{nullable}]",
                                assertion=assertion_lambda,
                                hint=hint_expr,
                            )
                        )
                if not nullable:
                    checks.append(
                        Check(
                            spark_session=self._spark_session,
                            level=CheckLevel.Error,
                            description="Schema Validation Null Check",
                        ).isComplete(column=column_name)
                    )

        return checks

    def _get_fulltable(self, db: str, tbl: str):
        if db:
            if "" == db:
                return tbl
            elif "" == db.strip(" "):
                return tbl
            else:
                return f"{db}.{tbl}"
        else:
            return tbl

    def _fetch_table_schema(self, catalog_type):
        """Fetch table schema using the appropriate catalog provider.

        Supports all catalog types: spark, hive, unity, glue.

        Args:
            catalog_type: Catalog type string from configuration.

        Returns:
            pyspark.sql.types.StructType for the table.

        Raises:
            ValueError: If catalog_type is not supported.
        """
        from dq.catalog.catalog_factory import CatalogFactory

        supported_types = [
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_SPARK,
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_HIVE,
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_UNITY,
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_GLUE,
        ]

        if catalog_type not in supported_types:
            raise ValueError(
                f"Unsupported catalog_type '{catalog_type}'. "
                f"Supported types: {', '.join(supported_types)}"
            )

        database = self._schema_config.get(
            constants.SCHEMA_VALIDATION_DATABASE_KEY, None
        )
        table = self._schema_config.get(constants.SCHEMA_VALIDATION_TABLE_KEY)
        catalog_name = self._schema_config.get("catalog", None)

        provider = CatalogFactory.get_provider(
            self._spark_session, catalog_type=catalog_type
        )
        return provider.get_table_schema(table, database=database, catalog=catalog_name)

    def _extract_basetype_and_length(self, datatype: str):
        """
        Extracts basetype and maximum length from provided datatype string (e.g. CharType(20), VarcharType(64)),
        Returns basetype and maxlength (None if no length is specifid).
        """
        match = re.match(r"(\w+Type)(\(([\d,]+)\))", datatype)
        if match:
            basedatatype = match.group(1)
            parameters = [int(re.sub(r"\D", "", i)) for i in match.group(2).split(",")]
            return basedatatype, parameters
        else:
            return datatype.rstrip("()"), None

    def apply_checks(self, df: DataFrame):
        """
        Automatically apply schema-level checks such as data type validation, nullable, unique,
        multi-column unique, foreign key validation (using dynamic list or join), and any additional constraints.
        """
        self._spark_session = df.sparkSession
        if self._single_check_mode:
            # All constraints will be part of a single Check object
            checks = Check(
                spark_session=self._spark_session,
                level=CheckLevel.Error,
                description="Schema Validation",
            )
        else:
            # Each constraint will have its own Check object
            checks = []

        catalog_type = self._schema_config.get(
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_KEY,
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_SPARK,
        )
        tableSchema = self._fetch_table_schema(catalog_type)
        if catalog_type == constants.SCHEMA_VALIDATION_CATALOG_TYPE_SPARK:
            checks = self._apply_spark_datatype_checks(
                checks=checks, tableSchema=tableSchema
            )
        elif catalog_type in [
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_GLUE,
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_UNITY,
            constants.SCHEMA_VALIDATION_CATALOG_TYPE_HIVE,
        ]:
            checks = self._apply_catalog_datatype_checks(
                checks=checks, tableSchema=tableSchema
            )

        # Apply multi-column UNIQUE constraints
        unique_constraints = self._schema_config.get(
            constants.SCHEMA_VALIDATION_UNQK_CONSTRAINTS, []
        )
        if unique_constraints:
            for unique_set in unique_constraints:
                if self._single_check_mode:
                    checks = checks.hasUniqueness(
                        unique_set,
                        lambda x: x == 1.0,
                        "Error: Unique Constraint Check failure",
                    )
                else:
                    checks.append(
                        Check(
                            spark_session=self._spark_session,
                            level=CheckLevel.Error,
                            description="Schema Validation Unique Check",
                        ).hasUniqueness(
                            unique_set,
                            lambda x: x == 1.0,
                            "Error: Unique Constraint Check failure",
                        )
                    )
        foreign_key_constraints = self._schema_config.get(
            constants.SCHEMA_VALIDATION_FK_CONSTRAINTS, []
        )
        if foreign_key_constraints:
            if self._single_check_mode:
                checks = self.create_foreign_key_checks(checks, df)
            else:
                checks.append(
                    self.create_foreign_key_checks(
                        Check(
                            spark_session=self._spark_session,
                            level=CheckLevel.Error,
                            description="Schema Validation Foregin Key Check",
                        ),
                        df,
                    )
                )
        return checks
