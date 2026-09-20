# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration loading and conversion utilities."""

from typing import Any, Dict, List, Optional, Union

from pyhocon import ConfigTree
import requests


def config_tree_to_python(config: Any) -> Any:
    """Recursively convert a HOCON ConfigTree to native Python types.

    Args:
        config: A ConfigTree, list, dict, or primitive value.

    Returns:
        The equivalent Python dict, list, or primitive.
    """
    if isinstance(config, ConfigTree):
        return config_tree_to_dict(config)
    elif isinstance(config, list):
        return [config_tree_to_python(item) for item in config]
    elif isinstance(config, dict):
        return {key: config_tree_to_python(value) for key, value in config.items()}
    else:
        return config


def config_tree_to_dict(config_tree: ConfigTree) -> Dict[str, Any]:
    """Recursively convert a ConfigTree into a regular Python dict.

    Args:
        config_tree: HOCON ConfigTree instance.

    Returns:
        Plain Python dictionary.
    """
    result = {}
    for key, value in config_tree.items():
        if isinstance(value, ConfigTree):
            result[key] = config_tree_to_dict(value)
        elif isinstance(value, list):
            result[key] = [
                config_tree_to_dict(item) if isinstance(item, ConfigTree) else item
                for item in value
            ]
        else:
            result[key] = value
    return result


def load_from_s3(bucket: str, key: str) -> str:
    """Load a configuration file from an S3 bucket.

    Args:
        bucket: S3 bucket name.
        key: Object key path within the bucket.

    Returns:
        File contents as a UTF-8 string.
    """
    import boto3

    session = boto3.session.Session()
    s3 = session.resource("s3")
    res = s3.Object(bucket, key)
    return res.get()["Body"].read().decode("utf-8")


def load_from_adls(uri: str) -> str:
    """Load a configuration file from Azure Data Lake Storage (ADLS Gen2).

    Uses the Hadoop filesystem available through the Spark context
    to read files from ``abfss://`` paths.

    Args:
        uri: Full ADLS URI (abfss://container@account.dfs.core.windows.net/path).

    Returns:
        File contents as a UTF-8 string.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found for ADLS access")
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    path = sc._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hadoop_conf)
    input_stream = fs.open(path)
    reader = sc._jvm.java.io.BufferedReader(
        sc._jvm.java.io.InputStreamReader(input_stream, "UTF-8")
    )
    lines = []
    line = reader.readLine()
    while line is not None:
        lines.append(line)
        line = reader.readLine()
    reader.close()
    return "\n".join(lines)


def load_from_uri(uri: str) -> Optional[str]:
    """Load a configuration file from an HTTP/HTTPS URI.

    Args:
        uri: Full URL to fetch.

    Returns:
        Response text if successful, None otherwise.
    """
    response = requests.get(uri, timeout=30)
    if 200 == response.status_code:
        return response.text
    else:
        return None
