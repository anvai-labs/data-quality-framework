# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from dq.validate import is_local_config_reference, validate_config


def write_config(tmp_path, body):
    path = tmp_path / "rules.conf"
    path.write_text(body, encoding="utf-8")
    return str(path)


def test_validate_config_rejects_empty_rule_set(tmp_path):
    path = write_config(tmp_path, "dqframework { dqrules = [] }\n")
    assert validate_config(path) is False


def test_validate_config_rejects_rule_without_checks(tmp_path):
    path = write_config(
        tmp_path,
        'dqframework { dqrules = [{ engine = "deequ", checks = [] }] }\n',
    )
    assert validate_config(path) is False


def test_validate_config_rejects_unvalidated_remote_source():
    assert validate_config("https://example.test/rules.conf") is False


def test_local_config_reference_detection():
    assert is_local_config_reference("rules.conf") is True
    assert is_local_config_reference("file:///tmp/rules.conf") is True
    assert is_local_config_reference("s3://bucket/rules.conf") is False


def test_validate_config_accepts_executable_local_rule_set(tmp_path):
    path = write_config(
        tmp_path,
        """
        dqframework {
          dqrules = [{
            engine = "deequ"
            checks = [{ constraint = "hasSize", value = 1 }]
          }]
        }
        """,
    )
    assert validate_config(path) is True
