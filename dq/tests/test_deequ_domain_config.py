# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyhocon import ConfigFactory
from dq.engine.deequ.deequ_engine import DeequEngine
from dq.engine.deequ.deequ_check import DeequCheck

from dq.dq_framework import DQFramework
from dq.tests.helpers import assert_overall_success


@pytest.fixture
def sample_domain_config():
    config_file = "file://./dq/tests/resources/sample_domain_validation.conf"
    return config_file


def test_deequ_engine_success(spark, multi_column_dataframe, sample_domain_config):
    dqf = DQFramework(spark, sample_domain_config, multi_column_dataframe)
    cum_metris = dqf.run()
    assert_overall_success(cum_metris, "deequ domain configuration")


pytestmark = pytest.mark.spark
